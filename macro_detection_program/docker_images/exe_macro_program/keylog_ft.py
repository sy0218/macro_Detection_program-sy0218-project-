def _key_time_ft(n_list):
    total_length = len(n_list)
    n_dict = {}
    for i in n_list:
        if i not in n_dict:
            n_dict[i] = 1
        else:
            n_dict[i] += 1

    same_keylog_time = 0
    for i in n_dict.values():
        if i != 1:
            same_keylog_time += i
    return round((same_keylog_time / total_length) * 100, 2)
