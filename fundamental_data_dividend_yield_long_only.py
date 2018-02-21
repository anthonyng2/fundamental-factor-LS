"""
Trading Strategy using Fundamental Data
1. Look at stocks in the Q1500US.
2. Go long in the top 100 stocks by dividend yield.
3. Rebalance once a month at market open.
"""

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data import Fundamentals, morningstar
from quantopian.pipeline.factors import CustomFactor
from quantopian.pipeline.filters import Q1500US
from quantopian.pipeline.factors import BusinessDaysSincePreviousEvent


def initialize(context):
    
    # Rebalance monthly on the first day of the month at market open
    schedule_function(rebalance,
                      date_rule=date_rules.month_start(days_offset=1),
                      time_rule=time_rules.market_open())

    attach_pipeline(make_pipeline(), 'fundamentals_pipeline')


# Custom Factor 1 : Dividend Yield
class Div_Yield(CustomFactor):

    inputs = [morningstar.valuation_ratios.dividend_yield]
    window_length = 1

    def compute(self, today, assets, out, d_y):
        out[:] = d_y[-1]

    
def make_pipeline():

    universe = Q1500US()
    div_yield = Div_Yield()
    
    # Top 100 and bottom 100 stocks ranked by dividend yield
    top_div_yield_stocks = div_yield.top(100, mask=universe)
    
    # Screen to include only securities tradable for the day
    securities_to_trade = (top_div_yield_stocks)
    
    pipe = Pipeline(
              columns={
                'div_yield': div_yield,
                'longs': top_div_yield_stocks,
              },
              screen = securities_to_trade
          )

    return pipe



"""
Runs our fundamentals pipeline before the marke opens every day.
"""
def before_trading_start(context, data): 

    context.pipe_output = pipeline_output('fundamentals_pipeline')

    # The stocks that we want to long.
    context.longs = context.pipe_output[context.pipe_output['longs']].index

    
    
    
def rebalance(context, data):
    
    my_positions = context.portfolio.positions
    
    # If we have at least one long and at least one short from our pipeline. Note that
    # we should only expect to have 0 of both if we start our first backtest mid-month
    # since our pipeline is scheduled to run at the start of the month.
    if (len(context.longs) > 0) :

        # Weight.
        long_weight = 1./len(context.longs)
        
        # Get our target names for our long baskets. We can display these later.
        target_long_symbols = [s.symbol for s in context.longs]

        log.info("Opening long positions each worth %.2f of our portfolio in: %s" \
                 % (long_weight, ','.join(target_long_symbols)))
        
        # Open long positions in our stocks.
        for security in context.longs:
            if data.can_trade(security):
                if security not in my_positions:
                    order_target_percent(security, long_weight)
            else:
                log.info("Didn't open long position in %s" % security)
                

    closed_positions = []
    
    # Close our previous positions that are no longer in our pipeline.
    for security in my_positions:
        if security not in context.longs \
        and data.can_trade(security):
            order_target_percent(security, 0)
            closed_positions.append(security)
    
    log.info("Closing our positions in %s." % ','.join([s.symbol for s in closed_positions]))