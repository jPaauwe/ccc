import pandas as pd
import numpy as np
import helpers as hlp
import time
import multiprocessing

def getRawReturnsData(user):
    import os
    import time
    
    tic = time.time()
    print("Starting retrieval of raw returns.")
    
    import wrds
    
    db = wrds.Connection(wrds_username = user)
    
    returns = db.get_table('crsp_a_stock', 'msf', columns=['permno, cusip, permco, issuno, hexcd, hsiccd, date, prc, ret, shrout, spread'])
    s = pd.DataFrame(columns=['permno', 'cusip', 'permco', 'issuno', 'hexcd', 'hsiccd', 'date', 'prc', 'ret', 'shrout', 'spread'], data=returns)
    s['retInternalIndex'] = s.index
    toc = time.time()
    print(s)
    print(f"Finished getting raw returns. {toc-tic:.3f} sec. elapsed.")
    s.to_pickle('returns.pkl')
    toc = time.time()
    #s = s.drop(['Unnamed: 0'], axis=1)
    return s

def getRawFundamentalsAnnualData(user):
    import os
    import time
    import helpers as hlp
    
    tic = time.time()
    task = 'getting raw fundamentals (annual)'
    hlp.printTaskStart(task)
    
    import wrds
    db = wrds.Connection(wrds_username = user)
    
    funda = db.get_table('comp', 'funda', columns=['gvkey, datadate, fyear, final, cusip, ib, at, oancf, dltt, act, lct, csho, revt, sale, cogs, bkvlps'])
    f = pd.DataFrame(columns=['gvkey', 'datadate', 'fyear', 'final', 'cusip', 'ib', 'at', 'oancf', 'dltt', 'act', 'lct', 'csho', 'revt', 'sale', 'cogs', 'bkvlps'], data=funda)
    f = f[f['final'] == 'Y']
    f = f.reset_index()
    f['funAInternalIndex'] = f.index
    
    file = 'funda-full.pkl'
    f.to_pickle(file)
    print(f)
    hlp.printTaskFinish(task,tic)
    return f

def addFundamentalsDateHelpers(fundamentals):
    dataDates = fundamentals['datadate'].values.tolist()
    fundamentals['datayear'] = list(map(lambda dataDates: int(str(dataDates)[0:4]), dataDates))
    fundamentals['datamonth'] = list(map(lambda dataDates: int(str(dataDates)[5:7]), dataDates))
    fundamentals['dataday'] = list(map(lambda dataDates: int(str(dataDates)[8:10]), dataDates))

    dataMonths = fundamentals['datamonth'].values.tolist()
    fundamentals['dataQ'] = list(map(lambda dataMonths: max(np.ceil(dataMonths / 3),1), dataMonths))
    return fundamentals

def calcTminusOne(fundamentals,varName):
    listToKeep = ['gvkey', 'funAInternalIndex', varName]
    listToRemove = hlp.keepOnly_ListToRemove(fundamentals,listToKeep)

    a = fundamentals.drop(listToRemove, axis=1)
    a = a.set_index('funAInternalIndex').groupby('gvkey').shift()
    a.columns = [i + '_t-1' for i in a.columns]
    a.reset_index(inplace=True)

    fundamentals = fundamentals.merge(a, on='funAInternalIndex', how='inner')
    return fundamentals

def calcDelta(fundamentals,varName):
    fundamentals = calcTminusOne(fundamentals,varName)
    dVarName = 'd'+varName
    tVarName = varName+'_t-1'
    fundamentals[dVarName] = fundamentals[varName] - fundamentals[tVarName]
    return fundamentals

def setIndicatorVariable(fundamentals,varName):
    fName = 'F_'+varName
    fundamentals[fName] = float('NaN')
    dataList = fundamentals[varName].values.tolist()
    fundamentals[fName] = list(map(lambda dataList: np.where(dataList > 0,1,0), dataList))
    return fundamentals

def setDeltaIndicatorVariable(fundamentals,varName):
    fName = 'F_d'+varName
    fundamentals[fName] = float('NaN')
    dataList = fundamentals[varName].values.tolist()
    fundamentals[fName] = list(map(lambda dataList: np.where(dataList > 0,1,0), dataList))
    return fundamentals

def equityOffer(fundamentals):
    varName = 'csho'
    fundamentals = calcTminusOne(fundamentals,varName)
    varName = 'csho_t-1'
    fundamentals = calcTminusOne(fundamentals,varName)
    fundamentals.rename(columns={"csho_t-1_t-1":"csho_t-2"}, inplace=True)
    t1 = fundamentals['csho_t-1'].values.tolist()
    t2 = fundamentals['csho_t-2'].values.tolist()
    fundamentals['EQ_OFFER'] = list(map(lambda t1,t2: np.where(t1<=t2,1,0), t1,t2))
    return fundamentals

def accrualIndicator(fundamentals):
    cfo = fundamentals['CFO'].values.tolist()
    roa = fundamentals['ROA'].values.tolist()
    
    fundamentals['F_ACCRUAL'] = list(map(lambda cfo,roa: np.where(cfo>roa,1,0), cfo,roa))
    return fundamentals

def main():
    file = 'funda-full.pkl'
    #fundamentals = pd.read_pickle(file)
    #fundamentals = getRawFundamentalsAnnualData('davidgitard91')
    #fundamentals = addFundamentalsDateHelpers(fundamentals)
    #print(fundamentals)

    task = 'Piotroski F-Score calculation (annual)'
    tic = time.time()
    hlp.printTaskStart(task)
    """
    for l in range(0,1):    #do the things
        # calculate funda bookeq
        fundamentals['BookValEq'] = fundamentals['bkvlps'] * fundamentals['csho']
        
        varName = 'at'
        fundamentals = calcTminusOne(fundamentals,varName)
        #print(fundamentals)

        # I use four variables to measure these performance-related (note: profitability) factors: ROA, CFO, dROA, and ACCRUAL. 
        # I define dTURN as the firm's current year asset turnover ratio (total sales scaled by beginning-of-the-year total assets) less the prior year's asset turnover ratio. An improvement in asset turnover signifies greater productivity from the asset base. 
        varName = 'TURN'
        fundamentals[varName] = fundamentals['revt'] / fundamentals['at_t-1']

        # I define ROA as net income before extraordinary items scaled by beginning-of-the-year total assets.
        varName = 'ROA'
        fundamentals[varName] = fundamentals['ib'] / fundamentals['at_t-1']

        # I define CFO as cash flow from operations scaled by beginning-of-the-year total assets.
        varName = 'CFO'
        fundamentals[varName] = fundamentals['oancf'] / fundamentals['at_t-1']

        varNameList = ['ROA', 'CFO']
        for j in range(0,len(varNameList)):
            fundamentals = setIndicatorVariable(fundamentals,varNameList[j])
        
        # I define the variable ACCRUAL as the current year's net income before extraordinary items less cash flow from operations, scaled by beginning-of-the-year total assets.
        #fundamentals['ACCRUAL'] = (fundamentals['ib'] - fundamentals['oancf']) / fundamentals['at_t-1']
        varName = 'ACCRUAL'
        fundamentals[varName] = fundamentals['ROA'] - fundamentals['CFO']

        # LEVERAGE
        varName = 'LEVER'
        fundamentals['at_avg'] = (fundamentals['at_t-1'] + fundamentals['at']) / 2
        fundamentals[varName] = fundamentals['dltt'] / fundamentals['at_avg']

        # LIQUIDITY
        varName = 'LIQUID'
        fundamentals[varName] = fundamentals['act'] / fundamentals['lct']
        
        # I define dMARGIN as the firm's current gross margin ratio (gross margin scaled by total sales) less the prior year's gross margin ratio. An improvement in margins signifies a potential improvement in factor costs, a reduction in inventory costs, or a rise in the price of the firm's product. 
        varName = 'MARGIN'
        fundamentals[varName] = fundamentals['ib'] / fundamentals['revt']
        
        # I define dROA as the current year's ROA less the prior year's ROA. 
        # If the firm's ROA (CFO) is positive, I define the indicator variable F_ROA (F_CFO) equal to one, zero otherwise.
        # If dROA > 0, the indicator variable F_dROA equals one, zero otherwise.
        # I measure dLEVER as the historical change in the ratio of total long-term debt to average total assets, and view an increase (decrease) in financial leverage as a negative (positive) signal. 
        # The variable dLIQUID measures the historical change in the firm's current ratio between the current and prior year, where I define the current ratio as the ratio of current assets to current liabilities at fiscal year-end. 
        
        varNameList = ['TURN', 'ROA', 'LEVER', 'LIQUID', 'MARGIN']
        for j in range(0,len(varNameList)):
            fundamentals = calcDelta(fundamentals,varNameList[j])
            fundamentals = setDeltaIndicatorVariable(fundamentals,varNameList[j])
        
        # I define the indicator variable EQ_OFFER as equal to one if the firm did not issue common equity in the year preceding portfolio formation, zero otherwise.
        fundamentals = equityOffer(fundamentals)
        
        # The indicator variable F_ACCRUAL equals one if CFO > ROA, zero otherwise.
        fundamentals = accrualIndicator(fundamentals)

        fundamentals = addFundamentalsDateHelpers(fundamentals)
        
        fileName = 'funda-pietroski2.pkl'
        fundamentals.to_pickle(fileName)
        print(fundamentals)
        hlp.printTaskFinish(task,tic)

        listToKeep = ['gvkey', 'datadate', 'F_ROA', 'F_dROA', 'F_CFO', 'F_ACCRUAL' ,'F_dLEVER' ,'F_dLIQUID' ,'EQ_OFFER' ,'F_dMARGIN', 'F_dTURN']
        listToRemove = hlp.keepOnly_ListToRemove(fundamentals,listToKeep)
        fundamentals.drop(listToRemove, axis=1, inplace=True)
        fileName = 'funda-pietroski-vars.pkl'
        fundamentals.to_pickle(fileName)
    print(fundamentals)
    """
    file = 'msf_linked.pkl'
    returns = pd.read_pickle(file)

    returns['MktValShare'] = abs(returns['prc']) * returns['shrout'] / 1000

    MVlist = []
    datelist = returns.drop_duplicates(subset=['date'])['date'].values.tolist()
    for i in range(0,len(datelist)):
        date = datelist[i]
        relevantReturns = returns[returns['date'] == date]
        #print(relevantReturns)
        
        series = relevantReturns.groupby('gvkey').apply((lambda x: x['MktValShare'].sum()))
        
        df = pd.DataFrame(data=[series])
        df = df.T
        
        MVlist.append(df)
        
        toc = time.time()
        print(f"Finished calculating MVs for date {datelist[i]} ({i} of {len(datelist)}). Processing is {round((100*i)/len(datelist),2)}% complete. \n Time spent processing: {int(toc-tic)} sec. Time remaining (est.): {int((len(datelist)-(i+1))*((toc-tic)/(i+1)))} sec.")

    def doSomeCalcs(retInternalIndex):
        relevantReturns = returns[returns['retInternalIndex'] == retInternalIndex]
        date = relevantReturns['date'].iloc[0]
        gvkey = relevantReturns['gvkey'].iloc[0]
        dateIndex = datelist.index(date)
        valToReturn = MVlist[dateIndex].loc[gvkey]
        if pd.isna(valToReturn):
            return float('NaN')
        else:
            return valToReturn
    
    import multiprocessing
    p = multiprocessing.Pool(8)
    
    retInternalIndices = returns['retInternalIndex'].values.tolist()
    returns['MktValEq'] = list(p.starmap(doSomeCalcs, retInternalIndices))
    
    print(returns)
    # get returns
    #link returns
    #calculate mktvaleq

    # Each year between 1976 and 1996, I identify firms with sufficient stock price and book value data on Compustat. 
    # For each firm, I calculate the market value of equity and BM ratio at fiscal year-end. 
    # Each fiscal year (i.e., financial report year), I rank all firms with sufficient data to identify 
    # book-to-market quintile and size tercile cutoffs. The prior fiscal year's BM distribution is used 
    # to classify firms into BM quintiles. Similarly, I determine a firm's size classification 
    # (small, medium, or large) using the prior fiscal year's distribution of market capitalizations. 
    # After the BM quintiles are formed, I retain firms in the highest BM quintile 
    # with sufficient financial statement data to calculate the various performance signals. 
    # This approach yields the final sample of 14,043 high BM firms across the 21 years.











if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    main()











