class Solution:
    def isPalindrome(self, s: str) -> bool:
        s = s.lower()
        p = ""
        for x in s:
            if x.isalnum() == True:
                p = p + x
        r = p[::-1]
        if p == r:
            return True
        else:
            return False