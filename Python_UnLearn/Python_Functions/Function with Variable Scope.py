def outer_function():
    x = 10
    
    def inner_function():
        nonlocal x
        x = 20
    
    inner_function()
    print("Inner function modified x:", x)

outer_function()

'''
This example demonstrates variable scope within nested functions. The inner_function modifies the value of a variable declared in the outer function using the nonlocal keyword.
'''