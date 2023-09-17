def factorial(request):
    user_input = request.args.get('input')
    if user_input is not None:
        userid, password = user_input.split()
    # ... Your code logic ...
        return "Success"  # Or your desired response
    else:
        return "Invalid input. Please provide a valid 'input' parameter."

