from pathlib import Path

# These are used to set colors of graphs
RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_PURPLE = 'rgb(160,106,255)'
RGB_GREY = 'rgb(200,200,200)'
RGB_PINK = 'rgb(255,182,193)'
#RGB_LIME = 'rgb(93, 239, 168)'
#RGB_LIME = 'rgb(88, 157, 15)'
RGB_LIME = 'rgb(17, 180, 101)'
#RGB_LIME = 'rgb(87, 159, 87)'

# These are used to set color of html tables via style argument
HEX_LBLUE = '#DAEBFF'
HEX_LGREE = '#DCFFDA'
HEX_LYELL = '#FFE4B3'
HEX_LREDD = '#FFDADA'
HEX_LGREY = '#EBEBEB'
HEX_LPURP = '#D1C0E5'
HEX_LPINK = '#FFB6C1'

# Give each status a color to display
QASTATUS2COLOR = {
    'PASS': RGB_GREEN,
    'NQA':  RGB_LIME,
    'NPUT': RGB_YELLOW,
    'FAIL': RGB_RED,
    'NONE': RGB_GREY,
    'JOBF': RGB_PINK,
    'JOBR': RGB_BLUE}

DEFAULT_COLOR = 'rgba(0,0,0,0.5)'

LINE_COLOR = 'rgba(50,50,50,0.9)'

STATUS2RGB = dict(zip(
    ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN', 'JOBF'],
    [RGB_GREY, RGB_YELLOW, RGB_GREEN, RGB_BLUE, RGB_RED, RGB_PURPLE, RGB_PINK]))

STATUS2HEX = dict(zip(
    ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN', 'JOBF'],
    [HEX_LGREY, HEX_LYELL, HEX_LGREE, HEX_LBLUE, HEX_LREDD, HEX_LPURP, HEX_LPINK]))

# These are used to make progress reports
ASTATUS2COLOR = {
    'PASS': RGB_GREEN,
    'NPUT': RGB_YELLOW,
    'FAIL': RGB_RED,
    'NQA': RGB_LIME,
    'NONE': RGB_GREY,
    'COMPLETE': RGB_BLUE,
    'UNKNOWN': RGB_PURPLE}

API_URL = 'https://redcap.vanderbilt.edu/api/'

KEYFILE = Path.home().joinpath('.redcap.txt')
