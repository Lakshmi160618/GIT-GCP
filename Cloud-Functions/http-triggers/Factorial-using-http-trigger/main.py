# main.py

def calculate_factorial(n):
    if n == 0:
        return 1
    else:
        return n * calculate_factorial(n-1)

def factorial(request):
    number = request.args.get('number')
    if number is not None:
        try:
            number = int(number)
            result = calculate_factorial(number)
            return f"Factorial of {number} is {result}"
        except ValueError:
            return "Invalid input. Please provide a valid number."
    else:
        return "Please provide a 'number' query parameter."
