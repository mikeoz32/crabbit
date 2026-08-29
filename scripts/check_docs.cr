require "json"

record MissingDoc,
  label : String,
  filename : String,
  line : Int32

GENERATED_RECORD_METHODS = {"new", "clone", "copy_with"}

def source_location(object : Hash(String, JSON::Any)) : Tuple(String, Int32)?
  locations = object["locations"]?.try(&.as_a?) || return
  location = locations.find do |candidate|
    candidate["filename"].as_s.starts_with?("src/crabbit")
  end
  return unless location
  {location["filename"].as_s, location["line_number"].as_i.to_i32}
end

def inspect_docs(
  node : JSON::Any,
  type_name : String = "Top Level",
  type_line : Int32? = nil,
  section : String = "",
  missing = [] of MissingDoc,
) : Array(MissingDoc)
  case node.raw
  when Hash
    object = node.as_h
    current_type = type_name
    current_type_line = type_line

    if full_name = object["full_name"]?.try(&.as_s?)
      if type_location = source_location(object)
        current_type = full_name
        filename, current_type_line = type_location
        unless object["doc"]? || object["program"]?.try(&.as_bool)
          missing << MissingDoc.new(full_name, filename, current_type_line)
        end
      end
    end

    if object["def"]? && (member_location = object["location"]?)
      filename = member_location["filename"].as_s
      line = member_location["line_number"].as_i.to_i32
      name = object["name"].as_s
      generated_record_method = line == current_type_line && GENERATED_RECORD_METHODS.includes?(name)
      if filename.starts_with?("src/crabbit") && !object["doc"]? && !generated_record_method
        marker = {"constructors", "class_methods"}.includes?(section) ? "." : "#"
        missing << MissingDoc.new("#{current_type}#{marker}#{name}", filename, line)
      end
    end

    object.each do |key, value|
      next_section = {"constructors", "class_methods", "instance_methods", "macros"}.includes?(key) ? key : section
      inspect_docs(value, current_type, current_type_line, next_section, missing)
    end
  when Array
    node.as_a.each do |value|
      inspect_docs(value, type_name, type_line, section, missing)
    end
  end
  missing
end

path = ARGV.first? || abort "usage: crystal run scripts/check_docs.cr -- <docs-index.json>"
missing = inspect_docs(JSON.parse(File.read(path))).uniq

if missing.empty?
  puts "All public Crabbit API declarations are documented"
else
  STDERR.puts "Undocumented public Crabbit API declarations:"
  missing.each { |item| STDERR.puts "- #{item.label} (#{item.filename}:#{item.line})" }
  exit 1
end
