import re

def _parse_self_sql(sql, old_table, new_table):
    # Step 1: Replace table definitions in FROM and JOIN clauses
    pattern_def = re.compile(
        r'\b(FROM|JOIN)(\s+)((?:\w+\.)?\w+(?:\s+(?:AS\s+)?\w+)?(?:\s*,\s*(?:\w+\.)?\w+(?:\s+(?:AS\s+)?\w+)?)*)',
        re.IGNORECASE | re.MULTILINE
    )

    def replace_definition(match):
        keyword = match.group(1)
        whitespace = match.group(2)
        tables_segment = match.group(3)

        tables = [t.strip() for t in re.split(r'\s*,\s*', tables_segment, flags=re.MULTILINE)]
        new_tables = []

        for t in tables:
            parts = t.split()
            table_name_part = parts[0]
            alias_part = ' '.join(parts[1:]) if len(parts) > 1 else ''

            if '.' in table_name_part:
                schema, name = table_name_part.split('.', 1)
            else:
                schema, name = '', table_name_part

            if name.lower() == old_table.lower():
                table_name_part = f"{schema + '.' if schema else ''}{new_table}"

            new_tables.append(f"{table_name_part} {alias_part}".rstrip())

        return f"{keyword}{whitespace}" + (', '.join(new_tables))

    sql = pattern_def.sub(replace_definition, sql)

    # Step 2: Replace all table references (self.n, self.*, etc.)
    # Only replace whole words followed by a dot
    pattern_ref = re.compile(rf'\b{re.escape(old_table)}\b(?=\.)', re.IGNORECASE)
    sql = pattern_ref.sub(new_table, sql)

    return sql