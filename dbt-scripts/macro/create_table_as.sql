{% macro create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {# backward compatibility for create_table_as that does not support language #}
  {% if language == "sql" %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code)}}
		 {%- if config.get('materialized')=='table' %}  
			alter table {{ relation }} add ("DBT_UPDATE_DATE" timestamp);
			alter table {{ relation }} add ("DBT_INSERT_DATE" timestamp);
			 update {{ relation }} set DBT_INSERT_DATE = current_timestamp();
		{%- endif %}

		{%- if config.get('scd2')==true %}  
			alter table {{ relation }} add ("DBT_UPDATE_DATE" timestamp);
			alter table {{ relation }} add ("DBT_INSERT_DATE" timestamp);
			update {{ relation }} set DBT_INSERT_DATE = current_timestamp();
		{%- endif %}
  
  {% else %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language) }}
  {% endif %}

{%- endmacro %}