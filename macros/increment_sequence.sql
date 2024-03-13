{%- macro increment_sequence() -%}
  
  dbt_syoon_timeless_shipping.seq_{{ this.name }}_key.nextval

{%- endmacro -%}