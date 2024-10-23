def arithmetic_mean(input_list):
    """Function to calculate the mean of a list of numbers.
    
    Args:
        input_list (list): A list of numbers.
    
    Returns:
        The mean of the list of numbers as a float.
        
    Raises:
        TypeError: if the data is not a list.
        ValueError: if the list is empty.
        ValueError: if the list is not all numbers.
    
    """
    if not isinstance(input_list, list):
        raise TypeError("The input data should be a list, it was a {}".format(
                                            type(input_list).__name__))
    if len(input_list) == 0:
        raise ValueError("The list is empty")
    if not all(isinstance(each_number, (int, float)) for each_number in input_list):
        raise TypeError("The list must contain ints and/or floats")
    value_sum = sum(input_list)
    number_of_values = len(input_list)
    mean_value = value_sum / number_of_values
    return mean_value
