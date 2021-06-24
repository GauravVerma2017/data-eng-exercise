from datetime import datetime, timedelta

dag_start_date = datetime.strptime("2021-01-24", '%Y-%m-%d')

schedule = {
    'file_merger': '0 4 * * *'
}

file_path = {
    'input_source_1': '/Users/gverma/PycharmProjects/data-eng-exercise/input_source_1/',
    'input_source_2': '/Users/gverma/PycharmProjects/data-eng-exercise/input_source_2/'
}

