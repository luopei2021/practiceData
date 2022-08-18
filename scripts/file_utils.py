class FileUtils:
    def __init__(self):
        pass

    @classmethod
    def getFileContent(cls, path):
        res = ""
        with open(path, 'r') as fr:
            for line in fr.readlines():
                res += line
        return res

    @classmethod
    def writeContentToFile(cls, path, content):
        with open(path, 'w') as fw:
            fw.write("\n".join(content))

    @classmethod
    def writeListToFile(cls, path, list):
        with open(path, 'w') as fw:
            fw.write("\n".join(list))
