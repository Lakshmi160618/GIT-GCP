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
# one more example
x = 10
print(x)
def func():
    x = 20 # Enclosed plac variable
    print(x)
    def inner_func1():
        x = 30 # non local
        print(x)
        return x
    res = inner_func1()
    return res

x = func()
print(x) 