

def flat_cate(categories_dic, c=1):
    """

    :param categories_dic:
        key: string  e.g: 'level1/level2/level3'
        value: double
    :param c: Update parent coefficient
    :return:
    """
    original_dic = dict(categories_dic)
    expansion_dic = dict({})

    lst_flat_cate = []
    for category in original_dic:
        lst_cate = category.split("/")
        n_level = len(lst_cate)

        #Create expanse category
        for i in range(1, n_level):
            lst_cate[i] = lst_cate[i-1] +"/" +lst_cate[i]
        print(lst_cate)

        if lst_cate[n_level-1] in expansion_dic:
            expansion_dic[lst_cate[n_level-1]] += original_dic[lst_cate[n_level-1]]
        else:
            expansion_dic[lst_cate[n_level - 1]] = original_dic[lst_cate[n_level - 1]]

        for i in range(n_level-1, 0, -1):
            current_cate = lst_cate[i]
            parent_cate = lst_cate[i-1]
            if parent_cate in expansion_dic:
                expansion_dic[parent_cate] += expansion_dic[current_cate] * c
            else:
                expansion_dic[parent_cate] = expansion_dic[current_cate] * c

    return expansion_dic

if __name__ == "__main__":

    cate = {
        "a/b/c/d" : 1,
        "a/b/e" : 1,
        "m/n/p" : 1,
        "m/r/s" : 1
    }

    flat = flat_cate(cate, 0.1)

    print(flat)