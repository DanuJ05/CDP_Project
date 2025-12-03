txt = "翨㒣䑴䡞冯掴荑蟌燯㐋齷㵛鞒⣠"

try:
    print("Result:", txt.encode('utf-16-le').decode('cp874'))
except:
    print("Try BE:", txt.encode('utf-16-be').decode('cp874'))