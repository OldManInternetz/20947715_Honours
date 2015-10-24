# Generates random data.
# Written by Michael Stewart 20947715.

require 'json'
require 'date'

$incremental_time = Time.local(2014, 1, 1, 0)
$incremental_id 	= 0
$model_titles 		= Array.new

$random_streets   = ["Struggle Street", "Hard Life Highway", "Hampden Road", "Pretend Road", "Broadway", "Fitzgerald Street", "Stirling Highway"]
$random_suburbs   = ["Crawley", "Nedlands"]

$random_addresses = Array.new

# Generates a random time, rounded to the nearest hour.
# http://stackoverflow.com/questions/4894198/how-to-generate-a-random-date-in-ruby
def time_rand from = 0.0, to = Time.now
  (Time.at(from + rand(to.to_f - from.to_f)).to_i / 3600.00).round * 3600
end

# Generates a random address.
def generate_random_address
	return "#{rand(0..200)}|#{$random_streets.sample}|#{$random_suburbs.sample}|Perth|WA"
end

# Generates a property value based on the values denoted in the JSON input file.
def get_property(property, values)

	case values
		when String, Array
			case values 
				when String then type = values 
				when Array  then type = values.first
			end
			case type
				when "<random_string>" 			then return ('a'..'z').to_a.shuffle[0, 8 + rand(5)].join.capitalize
				when "<random_integer>" 		then return rand(0..10)
				when "<incremental_id>"
					$incremental_id += 1
					return $incremental_id
				when "<random_timestamp>" 	then return time_rand Time.local(2015, 6, 7, 0), Time.local(2015, 6, 7, 12)
				when "<incremental_timestamp>"
					$incremental_time += 3600
					return $incremental_time.strftime('%s').to_i
				when "<random_address>"
					rand_address = generate_random_address
					while $random_addresses.include?(rand_address)
						rand_address = generate_random_address
					end
					$random_addresses << rand_address
					return rand_address
				else return values
			end
		when Hash 
			case values.keys.first
				when "<random_string>" 		then return values.values.first.sample		
				when "<random_integer>" 	then return rand(values.values.first.first.to_i .. values.values.first.last.to_i)
				when "<random_timestamp>" then return time_rand Time.local(values.values.first.first.to_i, 1, 1), Time.local(values.values.first.last.to_i, 1, 1)
				else raise "Error: values must be either <random_string>, <random_integer>, or <random_timestamp>."
			end
		else
			raise "Error: Syntax error in input JSON file."
	end
end

# Generates objects that you can put into a file
def generate_objects(models)
	# Create a new Hash to store the JSON objects
	json_objects = Hash.new

	# Add the title of each model into an array for referencing later
	models.each do |key, values|
		$model_titles    << key
	end

	models.each do |key, values|
		$incremental_id 	= 0
		model_title 			= key
		model_count 			= models[model_title]["count"]
		model_properties  = models[model_title]["properties"]

		json_objects[model_title] = Array.new
		# Iterate through each property again to generate a random value for each property
		for id in (1..model_count)
			object_properties = Hash.new
			json_objects[model_title] << object_properties
			if model_properties.nil?
				raise "Model has no properties."
			end

			model_properties.each do |property, values|
				# Check if the property name is actually a model title
				if $model_titles.include?(property)
					object_properties[property] = Hash.new
					values.each do |property2, values2|
						pr = get_property(property2, values2)
						object_properties[property][property2] = pr						
					end
				else
					pr = get_property(property, values)
					object_properties[property] = pr
				end
			end		
		end

	end

	return json_objects
end

# Read in the JSON file and parse it
file 	  = File.read("models.json")
models  = JSON.parse(file)

json_objects = generate_objects(models)

puts JSON.pretty_generate json_objects




