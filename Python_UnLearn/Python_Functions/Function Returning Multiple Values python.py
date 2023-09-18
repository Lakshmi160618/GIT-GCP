def get_name_and_initial(name):
    first_name = name.split()[0]
    initial = name[0]
    return first_name, initial

# Call the function
first_name, initial = get_name_and_initial("John Doe")
print("First Name:", first_name)
print("Initial:", initial)

'''
This function, get_name_and_initial, takes a full name as input and returns both the first name and the initial of the first name as separate values.
'''