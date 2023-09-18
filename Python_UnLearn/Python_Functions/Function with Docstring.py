def square(x):
    """
    This function squares a number.
    
    Parameters:
    x (int): The number to be squared.
    
    Returns:
    int: The square of the input number.
    """
    return x ** 2

# Access the function's docstring
print(square.__doc__)

# Call the function
result = square(4)
print("Square:", result)

"""
This example includes a function square with a docstring, which provides information about the function's purpose, parameters, and return value. You can access the docstring using the .__doc__ attribute.
"""