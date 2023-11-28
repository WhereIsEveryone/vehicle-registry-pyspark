import intro

# How many rows (in descending order) are stored/showed
show_items = 20

# A list to collect the string formed tables, which are later saved to a file
table_strings = []

# Converts a Spark dataframe to a dictionary, with a form of
# { 'table-metadata': { 'rows_count': int, 'columns_count': int, 'table_width': int },
# 'columns': {int[=order]: {'header': string, 'values': list, 'width':int }, int[=order]... } } 
def toDictTable(dframe):
    table_dict = {}
    row_count = dframe.count() if (dframe.count() <= show_items) else show_items
    col_count = len(dframe.columns)
    table_width = 0
    table_columns = {}
    order = 0
    #
    def getColWidth(values, header):
        str_values = list(map(lambda c: str(c), values))
        values_max_len = len(max(str_values, key=len))
        return len(header) if (len(header) >= values_max_len) else values_max_len
    #
    for clmn in range(col_count):
        col_prop = {}
        col_values = []
        col_name = dframe.dtypes[clmn][0]
        col_data = dframe.select(col_name)
        for row in range(row_count):
            col_values.append(col_data.collect()[row][0])
        col_prop['header'] = col_name
        col_prop['values'] = col_values
        col_width = getColWidth(col_values, col_name)
        col_prop['width'] = col_width
        table_width += col_width
        table_columns[order] = col_prop
        order += 1
    #
    table_metadata = {'rows_count': row_count, 'columns_count': col_count, 'table_width': table_width}
    table_dict['table_metadata'] = table_metadata
    table_dict['columns'] = table_columns
    return table_dict

# Creates a string from a dictionarized table, with a presentational table form
# which then can be printed or written "neatly"
def stringifyDictTable(table):
    metadata = table['table_metadata']
    columns = table['columns']
    width = metadata['table_width']
    width += (metadata['columns_count'] * 3) + 1
    row_count = metadata['rows_count']
    columns_count = metadata['columns_count']
    table_str = ''
    #
    def dash():
        dash_str = ''
        for i in range(width):
            dash_str += '-'
        dash_str += '\n'
        return dash_str
    table_str += dash()
    #
    for row_no in range(-1, row_count):
        for col_order in range(columns_count):
            clmn = columns[col_order]
            col_width = clmn['width']
            if (row_no == -1):
                header = clmn['header']
                table_str += (f'|{header.center(col_width+2)}')
            else:
                values = clmn['values']
                value = values[row_no]
                table_str += (f'|{str(value).center(col_width+2)}')
        table_str += '|\n'
        if (row_no == -1):
            table_str += dash()
    #
    table_str += dash()
    return table_str

# A hub to convert a dataframe to a presentational string and to store it to a list
def own_show(dframe):
    dict_table = toDictTable(dframe)
    str_table = stringifyDictTable(dict_table)
    print(str_table)
    table_strings.append(str_table)

# Writes the created presentational table strings to a file
def writeToFile(save_location):
    try:
        file = open(save_location, 'w', encoding='utf-8')
        try:
            file.write(intro.report_info())
            for tbl in table_strings:
                file.write(tbl)
                file.write('\n\n')
            try:
                file.close()
            except:
                print('Virhe tiedoston sulkemisessa')
        except:
                print('Virhe tiedostoon kirjoittamisessa. Kaikkia tietoja ei kirjoitettu tiedostoon')
    except:
        print('Virhe tallennustiedoston luomisessa. Tietoja ei kirjoiteta tiedostoon')