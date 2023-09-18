def divide(a: float, b: float) -> float:
    """
    This function divides two numbers.
    
    Parameters:
    a (float): The dividend.
    b (float): The divisor.
    
    Returns:
    float: The result of dividing a by b.
    """
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b

# Call the function
result = divide(10.0, 2.0)
print("Result:", result)

'''
This example defines a divide function with type hints in its parameters and return value. It also includes a docstring for documentation. The function performs division and raises an error if the divisor is zero.
'''