
def is_stop_indicator(indicator_name):
    flag = False
    flag = flag or indicator_name.startswith('停')
    return flag