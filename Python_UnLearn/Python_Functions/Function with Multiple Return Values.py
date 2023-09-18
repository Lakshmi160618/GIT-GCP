def get_name_and_age():
    name = "Alice"
    age = 30
    return name, age

# Call the function
result = get_name_and_age()
name, age = result
print("Name:", name)
print("Age:", age)

'''
In this example, the get_name_and_age function returns two values, name and age, as a tuple. When called, you can unpack the returned tuple into separate variables.
'''