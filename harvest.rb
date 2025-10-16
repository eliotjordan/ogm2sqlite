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

Document = Struct.new(
  :id,
  :title,
  :creator,
  :publisher,
  :description,
  :provider,
  :access_rights,
  :resource_class,
  :resource_type,
  :theme,
  :subject,
  :json
)

class Ogm2sqlite
  attr_reader :ogm_path, :logger, :db_path

  def initialize(ogm_path: "./tmp/ogm/", db_path: "./tmp/ogm.db", logger: Logger.new($stdout))
    @ogm_path = ogm_path
    @logger = logger
    @db_path = db_path
  end

  def convert
    setup_tables
    docs_to_ingest.each do |doc|
      logger.info(doc["id"])
      db.execute("insert into documents values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
                 create_document_struct(doc).to_a
                )
    end
  end

  private

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
  end

  def table_exists?(table_name)
    db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='#{table_name}';").any?
  end

  def create_document_struct(doc)
    Document.new(
      id: doc["id"],
      title: doc["dct_title_s"],
      creator: Array(doc["dct_creator_sm"]).first,
      publisher: Array(doc["dct_publisher_sm"]).first,
      description: Array(doc["dct_description_sm"]).first,
      provider: doc["schema_provider_s"],
      access_rights: doc["dct_accessRights_s"],
      resource_class: Array(doc["gbl_resourceClass_sm"]).first,
      resource_type: Array(doc["gbl_resourceType_sm"]).first,
      theme: Array(doc["dcat_theme_sm"]).first,
      subject: Array(doc["dct_subject_sm"]).first,
      json: doc.to_json
    )
  end

  def setup_documents
    db.execute <<-SQL
      create table documents (
        id varchar(255),
        title text,
        creator text,
        publisher text,
        description text,
        provider text,
        access_rights varchar(30),
        resource_class varchar(255),
        resource_type varchar(255),
        theme varchar(255),
        subject varchar(255),
        json text
      );
    SQL
  end

  def setup_bounds
    db.execute <<-SQL
      create virtual table bounds using geopoly(id);
    SQL
  end
end

Ogm2sqlite.new.convert
