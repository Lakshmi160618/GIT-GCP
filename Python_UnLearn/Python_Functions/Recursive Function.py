def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

# Calculate the factorial of 5
result = factorial(5)
print("Factorial:", result)

"""
This is an example of a recursive function that calculates the factorial of a number. It calls itself with a smaller argument until it reaches the base case (n == 0).
"""