import h3

def check_h3_resolution(h3_index):
    # Get the resolution of the H3 index
    resolution = h3.h3_get_resolution(h3_index)
    
    if resolution == 6:
        return f"{h3_index} is at H3_6 resolution."
    elif resolution == 8:
        return f"{h3_index} is at H3_8 resolution."
    elif resolution == 10:
        return f"{h3_index} is at H3_10 resolution."
    else:
        return f"{h3_index} is at resolution level {resolution}, not H3_6, H3_8, or H3_10."

# Example usage
h3_index_input = '862a100ffffffff'  # Replace with your input H3 index
result = check_h3_resolution(h3_index_input)
print(result)

