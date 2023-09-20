def get_name_and_age():
    name = "Alice"
    age = 30
    return name, age

# Call the function
result = get_name_and_age()
name, age = result # tuple unpacking
print("Name:", name)
print("Age:", age)

'''
In this example, the get_name_and_age function returns two values, name and age, as a tuple. When called, you can unpack the returned tuple into separate variables.
'''
def even_or_odd(num):
    if num%2==0:
        return "even number"
    else:
        return "odd number"

for x in range(10): # 10 times
    result = even_or_odd(x) # it is the line from which you are calling the function
    print(x,result)