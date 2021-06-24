import sys
import re
import math
import numpy as np

def is_bracketed_negative_num(original):
    if type(original) is not str:
        original = str(original)

    matches = re.findall(r"^\(\d+\.*\d*\)", original)
    if len(matches) == 0:
        return False
    else:
        return True

def extract_bracketed_negative_num(original):
     matches = re.findall(r"\(\d+\.*\d*\)", original)
     num = re.findall(r"\d+\.*\d*", matches[0])
     return -1 * float(num[0])

def is_number(s):
    try:
        if s is None:
            return False

        if not isinstance(s, str):
            s = str(s)

        tokens = [',','%']
        for token in tokens:
            s = s.replace(',', '') if s else ''

        if isinstance(s, np.datetime64):
            return False

        if is_bracketed_negative_num(s):
            extract_bracketed_negative_num(s)
        else:
            try:
                float(s)
            except:
                complex(s) # for int, long, float and complex
    except:
        return False
    return True

def is_nan(val):
    if not val:
        return True  
    try:
        val = float(val)    # if val==math.nan, then float(val)==math.nan, no exception here.
        return math.isnan(val) 
    except:
        # if s = "xxxjibberishxxx" then math.isnan would spit out except
        return False

def convert_float(s, default_val = -sys.maxsize):
    float_num = default_val
    
    if not isinstance(s, str):
        s = str(s)

    # Handle thousandth separator
    s = s.replace(',', '') if s else ''

    # Handle percentages
    div_factor = 1
    if '%' in s:
        s = s.replace('%','') if '%' in s else s
        div_factor = 100

    if is_number(s):
        try:
            if is_bracketed_negative_num(s):
                float_num = extract_bracketed_negative_num(s)
            else:
                float_num = float(s)

            float_num = float_num / div_factor

            if is_nan(float_num):
                float_num = default_val
        except:
            float_num = default_val
    
    return float_num