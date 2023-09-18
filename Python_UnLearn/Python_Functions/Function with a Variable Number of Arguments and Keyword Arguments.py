def describe_person(name, age, **kwargs):
    print(f"Name: {name}")
    print(f"Age: {age}")
    for key, value in kwargs.items():
        print(f"{key}: {value}")

# Call the function with variable keyword arguments
describe_person("Alice", 30, occupation="Engineer", city="New York")

'''
In this example, the describe_person function accepts two required parameters (name and age) and can receive an arbitrary number of keyword arguments using **kwargs. It then prints out the person's name and age, along with any additional information provided as keyword arguments.
'''