# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      # quote and internal_execute must be implemented
      module ConfigurationParameters
        private
          def ensure_parameter(name, value)
            return if parameter_set_to?(name, value)

            if block_given?
              yield value
            else
              set_parameter(name, value)
            end
          end

          def parameter_set_to?(name, value)
            validate_parameter!(name)

            if value == :default
              false # simplification
            else
              current_value = internal_execute("SHOW #{name}", "SCHEMA").getvalue(0, 0)
              # TODO: normalize depending on the type
              value == current_value
            end
          end

          def set_parameter(name, value)
            validate_parameter!(name)

            if value == :default
              internal_execute("SET SESSION #{name} TO DEFAULT", "SCHEMA")
            else
              internal_execute("SET SESSION #{name} TO #{quote(value)}", "SCHEMA")
            end
          end

          def validate_parameter!(name)
            raise ArgumentError, "Parameter name '#{name}' is invalid" unless name.match?(/\A[a-zA-Z0-9_.]+\z/)
          end
      end
    end
  end
end
