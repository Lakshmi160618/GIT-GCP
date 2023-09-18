def operate_on_numbers(x, y, operation):
    return operation(x, y)

# Define a lambda function for addition
addition = lambda a, b: a + b

# Call the function with the lambda function
result = operate_on_numbers(5, 3, addition)
print("Result of addition:", result)

'''
In this example, the operate_on_numbers function takes two numbers (x and y) and an operation (which is a lambda function). It then applies the specified operation to the numbers.
'''