class Solution:
    def longestCommonPrefix(self, strs: List[str]) -> str:
        strs = sorted(strs)
        print(strs)
        a = strs[0]
        b = strs[len(strs)-1]
        output = ""
        index = 0
        if len(a) >= len(b):
            for x in b:
                if x == a[index]:
                    output = output + x
                    index = index + 1
                else:
                    break
        else:
            for x in a:
                if x == b[index]:
                    output = output + x
                    index = index + 1
                else:
                    break
        return output