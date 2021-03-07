
# These are used to set colors of graphs
RGB_DKBLUE = 'rgb(59,89,152)'
RGB_BLUE = 'rgb(66,133,244)'
RGB_GREEN = 'rgb(15,157,88)'
RGB_YELLOW = 'rgb(244,160,0)'
RGB_RED = 'rgb(219,68,55)'
RGB_PURPLE = 'rgb(160,106,255)'
RGB_GREY = 'rgb(200,200,200)'

# These can be used to set color of html tables via style argument
HEX_LBLUE = '#DAEBFF'
HEX_LGREE = '#DCFFDA'
HEX_LYELL = '#FFE4B3'
HEX_LREDD = '#FFDADA'
HEX_LGREY = '#EBEBEB'
HEX_LPURP = '#D1C0E5'

# Give each status a color to display
QASTATUS2COLOR = {
    'PASS': RGB_GREEN,
    'TBD': RGB_YELLOW,
    'FAIL': RGB_RED,
    'NONE': RGB_GREY}

DEFAULT_COLOR = 'rgba(0,0,0,0.5)'

LINE_COLOR = 'rgba(50,50,50,0.9)'

STATUS2RGB = dict(zip(
    ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN'],
    [RGB_GREY, RGB_YELLOW, RGB_GREEN, RGB_BLUE, RGB_RED, RGB_PURPLE]))

STATUS2HEX = dict(zip(
    ['WAITING', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'UNKNOWN'],
    [HEX_LGREY, HEX_LYELL, HEX_LGREE, HEX_LBLUE, HEX_LREDD, HEX_LPURP]))