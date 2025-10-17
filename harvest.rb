# frozen_string_literal: true

require "openssl"
require "sqlite3"
require "json"
require "rsolr"
require "find"
require "debug"
require "faraday/net_http_persistent"
require "geo_combine/harvester"
require "geo_combine/indexer"
require "geo_combine/geo_blacklight_harvester"
load "lib/geometry.rb"

FIELD_MAP = {
  "dct_title_s" => "title",
  "dct_creator_sm" => "creator",
  "dct_publisher_sm" => "publisher",
  "dct_description_sm" =>  "description",
  "schema_provider_s" => "provider",
  "dct_accessRights_s" => "access_rights",
  "gbl_resourceClass_sm" => "resource_class",
  "gbl_resourceType_sm" => "resource_type",
  "dcat_theme_sm" => "theme",
  "dct_subject_sm" => "subject",
  "dct_spatial_sm" => "location",
  "dct_format_s" => "format",
  "dct_identifier_sm" => "identifier",
  "dct_references_s" => "references",
  "dct_temporal_sm" => "temporal",
  "gbl_wxsIdentifier_s" => "wxs_identifier",
  "gbl_mdModified_dt" => "modified",
  "locn_geometry" => "geometry",
  "dcat_bbox" => "bbox",
  "gbl_indexYear_im" => "index_year"
}

INDEX_FIELDS = [
  "title",
  "creator",
  "publisher",
  "description",
  "provider",
  "access_rights",
  "resource_class",
  "resource_type",
  "theme",
  "subject",
  "location"
]

FACET_FIELDS = [
  "provider",
  "access_rights",
  "resource_class",
  "resource_type",
  "theme",
  "location"
]

Document = Struct.new(
  :id,
  :access_rights,
  :creator,
  :description,
  :format,
  :identifier,
  :location,
  :provider,
  :publisher,
  :resource_class,
  :resource_type,
  :subject,
  :temporal,
  :theme,
  :title,
)

SearchDocument = Struct.new(
  :id,
  :data
)

class Ogm2sqlite
  attr_reader :ogm_path, :logger, :db_path

  def initialize(ogm_path: "./tmp/opengeometadata/", db_path: "./tmp/ogm.db", logger: Logger.new($stdout))
    @ogm_path = ogm_path
    @logger = logger
    @db_path = db_path
  end

  def convert
    setup_tables
    docs_to_ingest.each do |doc|
      begin
        logger.info(doc["id"])
        remapped_doc = remap_and_clean(doc)
        db.execute("insert into documents values ('#{doc["id"]}', jsonb('#{remapped_doc.to_json}'))")
        db.execute("insert into bounds values('#{coordinates(doc)}', '#{doc['id']}')")
        insert_fulltext_row(remapped_doc)
      rescue => e
        logger.warn("Error processing: #{doc["id"]}: #{e.message}")
        next
      end
    end

    create_indexes
  end

  private

  def remap_and_clean(doc)
    remapped = remap_doc_keys(doc)
    clean_values(remapped)
  end

  def remap_doc_keys(doc)
    doc.to_a.map do |k, v|
      new_key = FIELD_MAP[k]
      if new_key
        [ new_key, v ]
      else
        [ k, v ]
      end
    end.to_h
  end

  # Strip invalid characters from json
  def clean_values(doc)
    doc.to_a.map do |k, v|
      new_val = if v.is_a? Array
                  clean_array(v)
      elsif v.is_a? String
                  v.gsub("'", "")
      else
                  v
      end
      [ k, new_val ]
    end.to_h
  end

  def clean_array(value)
    value.map do |m|
      if m.is_a? String
        m.gsub("'", "")
      else
        m
      end
    end
  end

  def harvester
    @harvester ||= GeoCombine::Harvester.new(ogm_path: ogm_path)
  end

  def docs_to_ingest
    @docs_to_ingest ||= harvester.docs_to_index
  end

  def db
    @db ||= SQLite3::Database.new(db_path)
  end

  def setup_tables
    setup_documents unless table_exists?("documents")
    setup_bounds unless table_exists?("bounds")
    setup_document_store unless table_exists?("fulltext")
  end

  def table_exists?(table_name)
    db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='#{table_name}';").any?
  end

  def coordinates(doc)
    JSON.parse(geojson(doc))["coordinates"].first.to_s
  end

  def geojson(doc)
    Geometry.new(doc["dcat_bbox"]).geojson
  end

  def create_fulltext_struct(doc)
    processed = format_for_fulltext(doc)

    Document.new(
      id: processed["id"],
      access_rights: processed["access_rights"],
      creator: processed["creator"],
      description: processed["description"],
      format: processed["format"],
      identifier: processed["identifier"],
      location: processed["location"],
      provider: processed["provider"],
      publisher: processed["publisher"],
      resource_class: processed["resource_class"],
      resource_type: processed["resource_type"],
      subject: processed["subject"],
      temporal: processed["temporal"],
      theme: processed["theme"],
      title: processed["title"],
    )
  end

  def format_for_fulltext(doc)
    doc.to_a.map do |k, v|
      new_val = if v.is_a? Array
        v.reduce { |acc, val| acc + ", " +  val.to_s }
      else
        v.to_s
      end

      [ k, new_val ]
    end.to_h
  end

  def insert_fulltext_row(doc)
    fulltext_doc = create_fulltext_struct(doc)
    placeholders = fulltext_doc.members.map { |m| "?" }.join(", ")
    values = fulltext_doc.values
    query = "insert into fulltext values(#{placeholders})"
    db.execute(query, values)
  end

  def create_indexes
    [ "id", "data" ].each do |col|
      index_name = "documents_#{col}_idx"
      logger.info("Creating index #{index_name}")
      db.execute("create index #{index_name} on documents('#{col}')")
    end

    INDEX_FIELDS.each do |col|
      index_name = "documents_#{col}_idx"
      logger.info("Creating index #{index_name}")
      db.execute("create index #{index_name} on documents(jsonb_extract(data, '$.#{col}'))")
    end

    FACET_FIELDS.permutation(2).each do |fields|
      col1, col2 = fields
      index_name = "documents_#{col1}_#{col2}_idx"
      db.execute("create index #{index_name} on documents(jsonb_extract(data, '$.#{col1}'), jsonb_extract(data, '$.#{col2}'))")

      # Index for calculating facet counts. Right now only works for non-jsomb
      # strings. Returns doubles like: ["Web services","Datasets"] instead of
      # separating.
      index_name = "documents_#{col1}_#{col2}_count_idx"
      logger.info("Creating index #{index_name}")
      db.execute("create index #{index_name} on documents(jsonb_extract(data, '$.#{col1}'), json_extract(data, '$.#{col2}'))")
    end

    # Index three facets. Really balloons the size of the database.
    # FACET_FIELDS.permutation(3).each do |fields|
    #   col1, col2, col3 = fields
    #   index_name = "documents_#{col1}_#{col2}_#{col3}_idx"
    #   logger.info("Creating index #{index_name}")
    #   db.execute("create index #{index_name} on documents(jsonb_extract(json, '$.#{col1}'), jsonb_extract(json, '$.#{col2}'), jsonb_extract(json, '$.#{col3}'))")
    # end
  end

  def setup_documents
    db.execute <<-SQL
      create table documents (
        id varchar(255),
        data jsonb
      );
    SQL
  end

  def setup_bounds
    db.execute <<-SQL
      create virtual table bounds using geopoly(id);
    SQL
  end

  def setup_document_store
    db.execute <<-SQL
      create virtual table fulltext using fts5 (
        id,
        access_rights,
        creator,
        description,
        format,
        identifier,
        location,
        provider,
        publisher,
        resource_class,
        resource_type,
        subject,
        temporal,
        theme,
        title
      );
    SQL
  end
end

Ogm2sqlite.new.convert
