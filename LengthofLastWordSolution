class Solution:
    def lengthOfLastWord(self, s: str) -> int:
        reverseS = s[::-1]
        length = 0
        if reverseS[0] == " ":
            reverseS = reverseS.strip()
        if reverseS.isalpha() == True:
            return len(reverseS)
        for x in reverseS:
            if not x == " ":
                length = length + 1
            else:
                return length