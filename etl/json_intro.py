##Introduction to JSON File

# json file only stores string in "" not ''
# a way to differentiate between python dictionary that is in '' or json file which is in ""

from flatten_json import flatten
from pandas import DataFrame

weather_dict = {"today": {"temp_c": "23", "condition": "sunny"}, "tomorrow": {"temp_c": "24", "condition": "rainy"}}
weather = {"today": [{"temp_c": "23", "condition": "sunny"},{"temp_c": "24", "condition": "rainy"}]}
# a = ['23', 'sunny']

# temp = {"temp_c": 23 }
# temp2 = {"temp_c": 24}
# b = {'temperature': [temp, temp2]}
# new = weather_dict["today"]["temp_c"]
# print(new)

# for item in weather["today"]:
    
#     print(item['condition'])

# print(weather_dict['today']['condition'])

df_simple = flatten(weather)
print(df_simple)

df = DataFrame([df_simple])
print(df)