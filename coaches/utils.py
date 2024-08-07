import numpy

## utility func ##
def id_from_url(url):
    '''
    Pulls the coaches ID from the anchor tag
    '''
    try:
        return url.split('.htm')[0].split('/coaches/')[1]
    except:
        return numpy.nan