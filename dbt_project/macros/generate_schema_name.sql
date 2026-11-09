{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {# Đảm bảo luôn lấy chính xác tên custom schema (ví dụ: ll_silver, ll_gold) mà không bị cộng chuỗi #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}