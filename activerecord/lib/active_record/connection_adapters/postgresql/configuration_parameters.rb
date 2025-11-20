# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      # quote and internal_execute must be implemented
      module ConfigurationParameters
        private
          def parameter_set_to?(name, value)
            parameter = find_parameter(name)
            return false unless parameter

            if value == :default
              parameter[:setting] == parameter[:reset_val]
            else
              parameter[:setting] == value
            end
          end

          def find_parameter(name)
            return nil unless @pg_settings

            @pg_settings&.find { |parameter| parameter[:name].downcase == name.downcase }
          end

          def set_parameter(name, value)
            if value == :default
              internal_execute("SET SESSION #{name} TO DEFAULT", "SCHEMA")
            else
              # normalize name
              internal_execute("SET SESSION #{name} = #{quote(value)}", "SCHEMA")
            end
          end

          def ensure_parameter(name, value)
            return if parameter_set_to?(name, value)

            if block_given?
              yield value
            else
              set_parameter(name, value)
            end
          end

          def load_parameters
            rows = internal_execute(<<~SQL, "SCHEMA")
              SELECT name, setting, vartype, reset_val FROM pg_settings
            SQL

            @pg_settings = rows.map do |row|
              {
                name: row["name"],
                setting: row["setting"],
                reset_val: row["reset_val"],
                vartype: row["vartype"]
              }
            end
          end

          def reset_parameters
            @pg_settings = nil
          end
      end
    end
  end
end
