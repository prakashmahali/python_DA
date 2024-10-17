import h3

def get_h3_resolution(h3_index):
    """Return the resolution of the H3 index."""
    return h3.h3_get_resolution(h3_index)

def handle_h3_index(h3_index):
    """Handle input H3 index based on its resolution."""
    h3_res = get_h3_resolution(h3_index)
    
    if h3_res == 6:
        print(f"Input is H3_6: {h3_index}")
        return [h3_index]
    
    elif h3_res == 8:
        print(f"Input is H3_8: {h3_index}")
        h3_6 = h3.h3_to_parent(h3_index, 6)
        print(f"Parent H3_6 for H3_8 is: {h3_6}")
        
        # Get all H3_8 indices under this H3_6
        h3_8_list = h3.h3_to_children(h3_6, 8)
        return h3_8_list
    
    elif h3_res == 10:
        print(f"Input is H3_10: {h3_index}")
        h3_6 = h3.h3_to_parent(h3_index, 6)
        print(f"Parent H3_6 for H3_10 is: {h3_6}")
        
        # Get all H3_10 indices under this H3_6
        h3_10_list = h3.h3_to_children(h3_6, 10)
        return h3_10_list
    
    else:
        print(f"Unsupported H3 resolution: {h3_res}")
        return None

# Example Usage
input_h3_8 = '88283082b9fffff'  # Example H3_8 index
input_h3_10 = '8a283082b80ffff' # Example H3_10 index

# Handling H3_8 input
result_h3_8 = handle_h3_index(input_h3_8)
print(f"H3_8 List under H3_6 for {input_h3_8}: {result_h3_8}")

# Handling H3_10 input
result_h3_10 = handle_h3_index(input_h3_10)
print(f"H3_10 List under H3_6 for {input_h3_10}: {result_h3_10}")
