# Seeds data into OrientDB, and develops a concept hierarchy for timeseries data.
# Written by Michael Stewart 20947715.

require 'json'
require 'date'
require 'k_means'

$model_titles = Array.new

'''-Pseudocode-'''
# 1: Parse the input file data.
# 2: Generate SQL INSERT statements for each record.
# 3: While doing that, generate SQL INSERT statements for any Timeseries data.
#    -> Look at timestamp, convert it into Years, Months, Days, Hours, Minutes, Seconds (as far as possible)
#    -> Generate a SQL INSERT statement for a new vertex for any one of these time levels that hasn't been created already
#    -> Create an edge between the record with the timestamp, and the deepest time level's vertex
# 4: Do the same thing for locations (addresses, etc). Might be tricky, will need separation by a symbol or something
#    to determine the unique parts of the address.
#    -> Example: "230|Hampden Road|Crawley|Perth|WA" would be separated into:
#         230
#         Hampden Road
#         Crawley
#         Perth
#         WA
#       Fields for each of these properties are created and added to the model.
#       This will allow for the query of all houses belonging to a particular street, suburb, etc.
#       This would work for the Rammed Earth dataset as well. A wall inside a room inside a house inside a town would be
#       an example concept hierarchy.
# 5: Print all of the SQL INSERT statements to a file. 
#
# Other considerations:
# -> Need to account for different types of timestamps (strings, epochs etc).
# -> GPS coordinates? Not sure how that'll work.
'''------------'''

Timestamp = Struct.new(:year, :month, :day, :hour, :minute, :second)
$timestamp_array = Array.new
$edge_values     = Hash.new
$edge_clusters   = Hash.new   # A Hash of arrays, where each array contains the cluster name, the min, and max to be put into that cluster.

''' -------------------------------------------------------- Timestamps -------------------------------------------------------- '''

# Parses a timestamp and sends it to the create_time_objects function to be broken down into its different components
def parse_timestamp(timestamp)
  if timestamp.class == Fixnum
    ct = Time.at(timestamp.to_i)    # Converted timestamp
  else
    ct = DateTime.parse(timestamp)
  end
end

# Creates time component fields for the corresponding timestamp
def create_time_fields(timestamp, model)
  time_fields = Hash.new
  time_fields["Year"]     = timestamp.year
  time_fields["Month"]    = timestamp.month 
  time_fields["Day"]      = timestamp.day
  time_fields["Hour"]     = timestamp.hour 
  time_fields["Minute"]   = timestamp.min 
  time_fields["Second"]   = timestamp.sec
  time_fields["WeekDay"]  = timestamp.strftime("%A")
  return time_fields
end


''' -------------------------------------------------------- Addresses -------------------------------------------------------- '''

def parse_address(address)
  # Parse address
  return address.split("|")
end

def create_address_fields(address, model)

  # Assumes address is like: 222|TEST ST|SOUTH KALGOORLIE|LOT 2233

  address_fields = Hash.new
  address_fields["Lot"]             = (address[3].nil? or address[3].empty?) ? "NULL" : address[3]
  address_fields["Suburb"]          = (address[2].nil? or address[2].empty?) ? "NULL" : address[2]
  address_fields["Street"]          = (address[1].nil? or address[1].empty?) ? "NULL" : address[1]
  address_fields["Street Number"]   = (address[0].nil? or address[0].empty?) ? "NULL" : address[0]

  return address_fields

end


''' --------------------------------------------------------    Edges   -------------------------------------------------------- '''


# Generates a title for edges
def get_edge_title(from_class, to_class) "#{from_class}_#{to_class}" end

# Generates the edge class statements
def generate_sql_edge_class_statement(edge_title) "CREATE CLASS #{edge_title} EXTENDS E" end

# Generates the edge statements
def generate_sql_edge_record_statement(from_class, to_class, from_id, associations)
  edge_statements = Array.new
  edge_title = get_edge_title(from_class, to_class)
  associations.each do |id_value|

    id    = id_value.first
    value = id_value.last

    if value >= 0         # Don't create edges for value -1, as this means there is no edge.
      cluster = determine_cluster(edge_title, value)

      if cluster.nil?
        edge_statements << "CREATE EDGE #{edge_title} FROM (SELECT FROM #{from_class} WHERE id = #{from_id}) TO (SELECT FROM #{to_class} WHERE id = #{id}) SET Value = #{value}"
      else
        edge_statements << "CREATE EDGE #{edge_title} CLUSTER #{cluster} FROM (SELECT FROM #{from_class} WHERE id = #{from_id}) TO (SELECT FROM #{to_class} WHERE id = #{id}) SET Value = #{value}"
      end
    end
  end
  return edge_statements
end

# Generates the edge cluster statements
def generate_sql_edge_cluster_statements(edge_title)

  edge_cluster_statements = Array.new

  clusters = $edge_clusters[edge_title]

  # Generate SQL statements for each cluster
  clusters.each do |cluster|
    edge_cluster_statements << "ALTER CLASS #{edge_title} ADDCLUSTER #{cluster[0]}"    
  end

  return edge_cluster_statements

end

''' --------------------------------------------------------  Clusters  -------------------------------------------------------- '''


# Create clusters for an edge using the K-Means clustering algorithm
# K-Means gem was found here: https://github.com/reddavis/K-Means
def create_clusters(edge_title, num_centroids = 5)
  data = $edge_values[edge_title]

  kmeans  = KMeans.new(data, centroids: num_centroids)
  clusters = eval(kmeans.inspect)

  created_clusters = Array.new

  i = 0
  clusters.each do |cluster|
    created_clusters[i] = Array.new
    cluster.each do |value|
      created_clusters[i] << data[value].join(', ').to_i
    end
    created_clusters[i].sort!
    min = created_clusters[i].first
    max = created_clusters[i].last
    cluster_title = "#{edge_title}_#{min}_#{max}"
    created_clusters[i] = [cluster_title, min, max]
    i += 1
  end
  return created_clusters
end

# Determines which cluster to place a given value into
def determine_cluster(edge_title, value)
  clusters = $edge_clusters[edge_title]
  chosen_cluster = nil
  clusters.each do |cluster|
    if value >= cluster[1] and value <= cluster[2] and value >= 0
      chosen_cluster = cluster[0]
    end
  end
  return chosen_cluster 
end


''' --------------------------------------------------------  Vertices  -------------------------------------------------------- '''


# Generates an SQL vertex record statement for the given model and properties.
# Generates NULL and integers without speech marks.
def generate_sql_vertex_record_statement(model, properties)
  def to_field(value) value.class == String ? (value == "NULL" ? "#{value}" : "\"#{value}\"") : value end

  sql_statement_string = "INSERT INTO #{model} (#{properties.keys.to_s[1..-2].gsub(' ', '').gsub('(', '').gsub(')', '')}) VALUES ("

  properties.values.each do |value| ; sql_statement_string << "#{to_field(value)}, " ; end

  return sql_statement_string.chomp!(", ") << ")"
end

# Generates an SQL vertex property statement
def generate_sql_vertex_property_statement(model, property, value)
  property_class = value.class 
  if property_class == Hash
    property_class = "EmbeddedMap"
  elsif property_class == Fixnum
    property_class = "Integer"
  elsif property_class == FalseClass or property_class == TrueClass
    property_class = "Boolean"
  elsif property_class == NilClass
    property_class = "Any"
  end

  "CREATE PROPERTY #{model}.#{property} #{property_class}"
end


''' --------------------------------------------------------   Classes  -------------------------------------------------------- '''


# Generates the class statements
def generate_sql_class_statements(models)
  class_statements = Array.new
  models.each do |model, records|
    $model_titles << model
    class_statements << "CREATE CLASS #{model} EXTENDS V"
  end
  return class_statements
end


''' -------------------------------------------------------- Generation -------------------------------------------------------- '''


# Removes the "id" property from each class.
# There's no need for it - OrientDB has its own Record ID property. The only use of the "id" field is to allow associations
# to be made between records.
def generate_sql_remove_ids_statements(models)
  remove_ids_statements = Array.new
  models.keys.each do |model|
    remove_ids_statements << "DROP PROPERTY #{model}.id"
  end
  return remove_ids_statements
end

# Generates SQL statements that you can use to seed OrientDB
def generate_sql_statements(models)

  sql_statements = Hash.new
  sql_statements["Vertex_Classes"]    = Array.new
  sql_statements["Vertex_Properties"] = Array.new
  sql_statements["Vertex_Records"]    = Array.new
  sql_statements["Edge_Classes"]      = Array.new
  sql_statements["Edge_Clusters"]     = Array.new
  sql_statements["Edge_Records"]      = Array.new
  sql_statements["Spatial_Indeces"]   = Array.new
  sql_statements["Remove_IDs"]        = Array.new

  # Add the title of each model into an array for referencing later

  sql_statements["Vertex_Classes"] = generate_sql_class_statements(models)

  models.each do |model, records|
    
    first_record = true
    generate_clusters = false
    edge_title = nil

    # Add value of any associations to array
    records.each do |properties|
      properties.each do |property, value|
        property = property.gsub(' ', '').gsub('(', '').gsub(')', '')
        if $model_titles.include?(property)
          edge_title = get_edge_title(model, property)
          if first_record 
            $edge_values[edge_title] = Array.new
          end
          if value.first.last >= 0
            $edge_values[edge_title] << [value.first.last] 
          end
          generate_clusters = true
        end
      end
      first_record = false
    end

    # Create the clusters
    if generate_clusters
      $edge_clusters[edge_title]      = create_clusters(edge_title)
      sql_statements["Edge_Clusters"] = generate_sql_edge_cluster_statements(edge_title)
      generate_clusters = false 
    end

    first_record = true

    # Generate records
    records.each do |properties|

      record_id = properties["id"]

      properties.each do |property, value|

        property = property.gsub(' ', '').gsub('(', '').gsub(')', '')

        if property == "timestamp" or property == "Timestamp"
          # Parse the timestamp and add the fields to the model for easy querying
          time_fields = create_time_fields(parse_timestamp(value), model)
          properties  = properties.merge(time_fields)
        elsif property == "address" or property == "Address"
          # Parse the address and add the fields to the model for easy querying
          address_fields = create_address_fields(parse_address(value), model)
          properties  = properties.merge(address_fields)
          properties.delete (property)
        elsif property == "coordinates"
          sql_statements["Spatial_Indices"] << "CREATE INDEX #{model}.l_lon ON #{model}(#{property[0]}, #{property[1]}) SPATIAL INDEX LUCENE"
        # Check if association
        elsif $model_titles.include?(property)
          edge_title = get_edge_title(model, property)
          # Add this to the associated models, to generate edges for later.
          if first_record then sql_statements["Edge_Classes"] << generate_sql_edge_class_statement(edge_title) end
          sql_statements["Edge_Records"]  << generate_sql_edge_record_statement(model, property, record_id, value)
          # Delete it so that it does not generate a vertex
          properties.delete(property)
        end
      end

      # Generate the properties of this model if it is the first record
      properties.each do |property, value|
        property = property.gsub(' ', '').gsub('(', '').gsub(')', '')
        if first_record
          sql_statements["Vertex_Properties"] << generate_sql_vertex_property_statement(model, property, value)
        end  
      end
      first_record = false

      sql_statements["Vertex_Records"] << generate_sql_vertex_record_statement(model, properties)
    end
  end
  sql_statements["Remove_IDs"] = generate_sql_remove_ids_statements(models)

  return sql_statements

end

# Prints all the output.
def print_output(sql_statements)
  puts "connect remote:localhost/watercorp_database root password\n"
  puts "declare intent massiveinsert\n"

  puts   sql_statements["Vertex_Classes"]
  puts   sql_statements["Vertex_Properties"]
  puts   sql_statements["Vertex_Records"]
  puts   sql_statements["Edge_Classes"]
  puts   sql_statements["Edge_Clusters"]
  puts   sql_statements["Edge_Records"]
  puts   sql_statements["Spatial_Indices"]
  puts   sql_statements["Remove_IDs"]

end


# Read in the JSON file and parse it
file 	   = File.read("input.json")
models  = JSON.parse(file)

sql_statements = generate_sql_statements(models)

print_output(sql_statements)
