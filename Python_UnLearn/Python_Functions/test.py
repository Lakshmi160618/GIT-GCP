import unittest
from unittest.mock import patch

# Import the function you want to test
from FunctionwithNoParametersandNoReturnValue import greet  # Replace 'your_module' with the actual module name

class TestGreetFunction(unittest.TestCase):

    def test_greet_output(self):
        # Create a custom stream to capture the printed output
        captured_output = []

        def custom_print(*args, **kwargs):
            output = " ".join(map(str, args))
            captured_output.append(output)

        # Replace the print function with custom_print temporarily
        with patch('builtins.print', custom_print):
            # Call the function
            greet()

        # Restore the original print function
        self.assertEqual(captured_output, ["Helloo, world!"])

if __name__ == '__main__':
    unittest.main()
