from ..coaches import update_coach_meta
from ..stats import StatCompiler

def run():
    '''
    Updates the package by scraping coaching and then compiling stats
    '''
    ## this is an awful implimentation with no consistency ##
    update_coach_meta()
    s = StatCompiler()