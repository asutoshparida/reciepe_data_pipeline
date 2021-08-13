"""
CustomUDF.py
~~~~~~~~

Module containing helper function for UDFs
"""


class CustomUDF(object):
    """class for all spark UDFs
    """

    def convert_string_minutes(self, col_value):
        '''
        Convert string formatted hour minutes to minutes.(PT1H20M/PT1H/PT10M/PT)
        :param col_value:
        :return: return_value
        '''
        return_value = 0

        if col_value.strip() == '' or col_value.strip() == 'PT' or ('H' not in col_value and 'M' not in col_value):
            return_value = 0
        elif 'H' in col_value and 'M' in col_value:
            hour = col_value[col_value.index("PT") + 2:col_value.index("H")]
            minutes = col_value[col_value.index("H") + 1:col_value.index("M")]
            return_value = int(hour) * 60 + int(minutes)
        elif 'H' in col_value:
            hour = col_value[col_value.index("PT") + 2:col_value.index("H")]
            return_value = int(hour) * 60
        elif 'M' in col_value:
            minutes = col_value[col_value.index("PT") + 2:col_value.index("M")]
            return_value = int(minutes)

        return return_value

    def check_array_contains_str(self, col_value, str_to_check):
        '''
        check for specific string in a array column if yes then return 1 else 0
        :param col_value
        :param str_to_check
        :return: return_value
        '''
        return_value = 0
        if col_value is not None:
            for value in col_value:
                if str_to_check in value.lower():
                    return_value = 1
                    break

        return return_value
