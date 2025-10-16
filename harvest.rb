# frozen_string_literal: true

require "openssl"
require "sqlite3"
require "json"
require "rsolr"
require "find"
require "faraday/net_http_persistent"
require "geo_combine/harvester"
require "geo_combine/indexer"
require "geo_combine/geo_blacklight_harvester"

class Ogm2sqlite
  attr_reader :ogm_path, :logger, :db_path

  def initialize(ogm_path:, db_path: 'tmp/ogm.db', logger: Logger.new($stdout))
    @ogm_path = ogm_path
    @logger = logger
    @db_path = db_path
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
end
