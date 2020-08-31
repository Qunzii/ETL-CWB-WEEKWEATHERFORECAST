def fetchDataId(seq):

    otherDataId_list = ['F-C0032-005'] # 一週縣市天氣預報
    
    
    if seq < len(otherDataId_list):
        return otherDataId_list[seq]
    else:
        return ''