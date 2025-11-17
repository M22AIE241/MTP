# """
# This script syncs DDL changes from source to target tables based on mismatches:
# Case 1: Source table matches target table - No action required.
# Case 2: Source dropped columns - Drop in target.
# Case 3: Source changed datatype - Alter in target.
# Case 4: Source added columns - Add to target.
# Case 5: Source renamed a column - Rename in target.
# Case 6: Ensure audit columns are present in the target table.
# """

# from compare_ddl_enhanced import compare_snowflake_schemas, sf_db, sf_schema1, sf_schema2
# from connection_mod import snowflake_connection
# import pandas as pd

# # List of audit columns that must always be present in the target table
# AUDIT_COLUMNS = [
#     ('ETL_RECORD_PROCESS_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
#     ('ETL_RECORD_CAPTURE_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
#     ('ETL_RECORD_STATUS_CD', 'VARCHAR(10)', 'N')
#     # ('ETL_RECORD_STATUS_TIME', 'TIMESTAMP_LTZ(9)', 'N')
# ]

# def detect_column_renames(added_df, dropped_df):
#     """
#     Detect possible column renames by matching added and dropped columns with same datatype.
#     Returns a list of tuples: (old_column, new_column, datatype)
#     """
#     renames = []
#     used_dropped = set()
#     used_added = set()

#     for idx_added, added_row in added_df.iterrows():
#         for idx_dropped, dropped_row in dropped_df.iterrows():
#             if (
#                 idx_dropped not in used_dropped and
#                 added_row['SC_DATATYPE'] == dropped_row['TG_DATATYPE']
#             ):
#                 renames.append((dropped_row['COLUMN'], added_row['COLUMN'], added_row['SC_DATATYPE']))
#                 used_dropped.add(idx_dropped)
#                 used_added.add(idx_added)
#                 break

#     added_df_filtered = added_df.drop(index=used_added)
#     dropped_df_filtered = dropped_df.drop(index=used_dropped)

#     return renames, added_df_filtered, dropped_df_filtered


# # Fetch tables needing sync
# sync_list, missing_in_schema2, extra_in_schema2 = compare_snowflake_schemas()
# print(f"Tables to be synced: {sync_list}")

# for table in sync_list:
#     print(f"\n🔄 Processing table: {table}")
#     cursor = snowflake_connection.cursor()

#     # Source table structure
#     cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema1}.{table}")
#     sc_df = pd.DataFrame(
#         [(row[0].upper(), row[1].upper(), row[5].upper()) for row in cursor.fetchall()],
#         columns=['COLUMN', 'SC_DATATYPE', 'SC_PKEY']
#     )
#     print("\n📘 Source Table Structure:")
#     print(sc_df)

#     # Target table structure
#     cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema2}.{table}")
#     tg_df = pd.DataFrame(
#         [(row[0].upper(), row[1].upper(), row[5].upper()) for row in cursor.fetchall()],
#         columns=['COLUMN', 'TG_DATATYPE', 'TG_PKEY']
#     )
#     print("\n📙 Target Table Structure:")
#     print(tg_df)

#     alter_statements = []

#     # Ensure audit columns are present in the target table
#     target_columns = set(tg_df['COLUMN'])
#     missing_audit_columns = [col for col in AUDIT_COLUMNS if col[0] not in target_columns]
#     for col_name, col_type, _ in missing_audit_columns:
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD COLUMN {col_name} {col_type};"
#         )
#         print(f"🔧 Missing audit column '{col_name}' will be added to the target table.")

#     # Merge source and target
#     merged_df = pd.merge(sc_df, tg_df, how="outer", on="COLUMN", indicator=True, suffixes=('_SC', '_TG'))

#     # Identify added and dropped columns
#     added_columns = merged_df[merged_df['_merge'] == 'left_only']
#     dropped_columns = merged_df[merged_df['_merge'] == 'right_only']

#     # Detect renames
#     renames, added_columns, dropped_columns = detect_column_renames(added_columns, dropped_columns)
#     if renames:
#         print("\n🔁 Detected Renamed Columns:")
#         for old_col, new_col, _ in renames:
#             print(f"{old_col} → {new_col}")
#             alter_statements.append(
#                 f"ALTER TABLE {sf_db}.{sf_schema2}.{table} RENAME COLUMN {old_col} TO {new_col};"
#             )

#     # Add new columns
#     for _, row in added_columns.iterrows():
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD COLUMN {row['COLUMN']} {row['SC_DATATYPE']};"
#         )

#     # Drop removed columns
#     for _, row in dropped_columns.iterrows():
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} DROP COLUMN {row['COLUMN']};"
#         )

#     # Datatype mismatches
#     datatype_mismatches = merged_df[
#         (merged_df['_merge'] == 'both') & (merged_df['SC_DATATYPE'] != merged_df['TG_DATATYPE'])
#     ]
#     for _, row in datatype_mismatches.iterrows():
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ALTER COLUMN {row['COLUMN']} SET DATA TYPE {row['SC_DATATYPE']};"
#         )

#     # Add primary keys
#     pk_add = merged_df[
#         (merged_df['SC_PKEY'] == 'Y') & (merged_df['TG_PKEY'] != 'Y')
#     ]
#     for _, row in pk_add.iterrows():
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD PRIMARY KEY ({row['COLUMN']});"
#         )

#     # Drop primary key (Snowflake: drop full constraint)
#     pk_drop = merged_df[
#         (merged_df['SC_PKEY'] != 'Y') & (merged_df['TG_PKEY'] == 'Y')
#     ]
#     if not pk_drop.empty:
#         alter_statements.append(
#             f"ALTER TABLE {sf_db}.{sf_schema2}.{table} DROP PRIMARY KEY;"
#         )

#     # Show and optionally execute DDLs
#     if alter_statements:
#         print(f"\n🛠️ Generated ALTER TABLE statements for '{table}':")
#         for stmt in alter_statements:
#             print(stmt)

#         if input("⚠️ Apply DDL changes? (Y/N): ").strip().upper() == 'Y':
#             for stmt in alter_statements:
#                 cursor.execute(stmt)
#             print(f"✅ Changes applied to table '{table}'.")

#             # Create or replace stream after applying DDL changes
#             regenerate_stream_query = f"CREATE OR REPLACE STREAM {sf_db}.{sf_schema1}.{table}_stream_type1 ON TABLE {sf_db}.{sf_schema1}.{table} SHOW_INITIAL_ROWS = TRUE;"
#             print("🔄 Capturing DDL changes in Stream:\n", regenerate_stream_query)
#             cursor.execute(regenerate_stream_query)
#         else:
#             print(f"⏩ Skipped applying changes for '{table}'.")
#     else:
#         print(f"✅ No DDL changes required for '{table}'.")



"""
This script syncs DDL changes from source to target tables based on mismatches:
Case 1: Source table matches target table - No action required.
Case 2: Source dropped columns - Drop in target.
Case 3: Source changed datatype - Alter in target.
Case 4: Source added columns - Add to target.
Case 5: Source renamed a column - Rename in target.
Case 6: Ensure audit columns are present in the target table.
"""

from compare_ddl_enhanced import compare_snowflake_schemas, sf_db, sf_schema1, sf_schema2
from connection_mod import snowflake_connection
import pandas as pd

# List of audit columns that must always be present in the target table
AUDIT_COLUMNS = [
    ('ETL_RECORD_PROCESS_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
    ('ETL_RECORD_CAPTURE_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
    ('ETL_RECORD_STATUS_CD', 'VARCHAR(10)', 'N')
]

def detect_column_renames(added_df, dropped_df):
    """
    Detect possible column renames by matching added and dropped columns with same datatype.
    Returns a list of tuples: (old_column, new_column, datatype)
    """
    renames = []
    used_dropped = set()
    used_added = set()

    for idx_added, added_row in added_df.iterrows():
        for idx_dropped, dropped_row in dropped_df.iterrows():
            if (
                idx_dropped not in used_dropped and
                added_row['SC_DATATYPE'] == dropped_row['TG_DATATYPE']
            ):
                renames.append((dropped_row['COLUMN'], added_row['COLUMN'], added_row['SC_DATATYPE']))
                used_dropped.add(idx_dropped)
                used_added.add(idx_added)
                break

    added_df_filtered = added_df.drop(index=used_added)
    dropped_df_filtered = dropped_df.drop(index=used_dropped)

    return renames, added_df_filtered, dropped_df_filtered


# Fetch tables needing sync
sync_list, missing_in_schema2, extra_in_schema2 = compare_snowflake_schemas()
print(f"Tables to be synced: {sync_list}")

for table in sync_list:
    print(f"\n🔄 Processing table: {table}")
    cursor = snowflake_connection.cursor()

    # Source table structure
    cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema1}.{table}")
    sc_df = pd.DataFrame(
        [(row[0].upper(), row[1].upper(), row[5].upper()) for row in cursor.fetchall()],
        columns=['COLUMN', 'SC_DATATYPE', 'SC_PKEY']
    )
    print("\n📘 Source Table Structure:")
    print(sc_df)

    # Target table structure
    cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema2}.{table}")
    tg_df = pd.DataFrame(
        [(row[0].upper(), row[1].upper(), row[5].upper()) for row in cursor.fetchall()],
        columns=['COLUMN', 'TG_DATATYPE', 'TG_PKEY']
    )
    print("\n📙 Target Table Structure:")
    print(tg_df)

    alter_statements = []

    # Ensure audit columns are present in the target table
    target_columns = set(tg_df['COLUMN'])
    missing_audit_columns = [col for col in AUDIT_COLUMNS if col[0] not in target_columns]
    for col_name, col_type, _ in missing_audit_columns:
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD COLUMN {col_name} {col_type};"
        )
        print(f"🔧 Missing audit column '{col_name}' will be added to the target table.")

    # Merge source and target
    merged_df = pd.merge(sc_df, tg_df, how="outer", on="COLUMN", indicator=True, suffixes=('_SC', '_TG'))

    # Exclude audit columns from being dropped
    audit_column_names = [col[0] for col in AUDIT_COLUMNS]
    dropped_columns = merged_df[
        (merged_df['_merge'] == 'right_only') & (~merged_df['COLUMN'].isin(audit_column_names))
    ]

    # Identify added columns
    added_columns = merged_df[merged_df['_merge'] == 'left_only']

    # Detect renames
    renames, added_columns, dropped_columns = detect_column_renames(added_columns, dropped_columns)
    if renames:
        print("\n🔁 Detected Renamed Columns:")
        for old_col, new_col, _ in renames:
            print(f"{old_col} → {new_col}")
            alter_statements.append(
                f"ALTER TABLE {sf_db}.{sf_schema2}.{table} RENAME COLUMN {old_col} TO {new_col};"
            )

    # Add new columns
    for _, row in added_columns.iterrows():
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD COLUMN {row['COLUMN']} {row['SC_DATATYPE']};"
        )

    # Drop removed columns (excluding audit columns)
    for _, row in dropped_columns.iterrows():
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} DROP COLUMN {row['COLUMN']};"
        )

    # Datatype mismatches
    datatype_mismatches = merged_df[
        (merged_df['_merge'] == 'both') & (merged_df['SC_DATATYPE'] != merged_df['TG_DATATYPE'])
    ]
    for _, row in datatype_mismatches.iterrows():
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ALTER COLUMN {row['COLUMN']} SET DATA TYPE {row['SC_DATATYPE']};"
        )

    # Add primary keys
    pk_add = merged_df[
        (merged_df['SC_PKEY'] == 'Y') & (merged_df['TG_PKEY'] != 'Y')
    ]
    for _, row in pk_add.iterrows():
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} ADD PRIMARY KEY ({row['COLUMN']});"
        )

    # Drop primary key (Snowflake: drop full constraint)
    pk_drop = merged_df[
        (merged_df['SC_PKEY'] != 'Y') & (merged_df['TG_PKEY'] == 'Y')
    ]
    if not pk_drop.empty:
        alter_statements.append(
            f"ALTER TABLE {sf_db}.{sf_schema2}.{table} DROP PRIMARY KEY;"
        )

    # Show and optionally execute DDLs
    if alter_statements:
        print(f"\n🛠️ Generated ALTER TABLE statements for '{table}':")
        for stmt in alter_statements:
            print(stmt)

        if input("⚠️ Apply DDL changes? (Y/N): ").strip().upper() == 'Y':
            for stmt in alter_statements:
                cursor.execute(stmt)
            print(f"✅ Changes applied to table '{table}'.")

            # Create or replace stream after applying DDL changes
            regenerate_stream_query = f"CREATE OR REPLACE STREAM {sf_db}.{sf_schema1}.{table}_stream_type1 ON TABLE {sf_db}.{sf_schema1}.{table} SHOW_INITIAL_ROWS = TRUE;"
            print("🔄 Capturing DDL changes in Stream:\n", regenerate_stream_query)
            cursor.execute(regenerate_stream_query)
        else:
            print(f"⏩ Skipped applying changes for '{table}'.")
    else:
        print(f"✅ No DDL changes required for '{table}'.")