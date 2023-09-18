def calculate_sum(*args):
    total = sum(args)
    return total

# Call the function with varying numbers of arguments
result1 = calculate_sum(1, 2, 3)
result2 = calculate_sum(10, 20, 30, 40, 50)

print("Sum 1:", result1)
print("Sum 2:", result2)

"""
In this example, the calculate_sum function uses *args to accept a variable-length list of arguments. It calculates and returns the sum of all the arguments passed to it.
"""