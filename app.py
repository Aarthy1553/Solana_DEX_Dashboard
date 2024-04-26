import asyncio
import pandas as pd
import streamlit as st
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport


async def run_subscription():
    transport = WebsocketsTransport(
        url="wss://streaming.bitquery.io/eap?token=ory_at_bOnOVSooos0AGEJsaVZFfw8K7b3L3ZqjmIxeCSyheP8.X0YNqJ2rSg8TlMB9EZrGY-YesYT-jYLDam_31cTau_4",
        headers={"Sec-WebSocket-Protocol": "graphql-ws"}
    )


    await transport.connect()
    print("connected")


    try:
        general_df = pd.DataFrame()
        raydium_df = pd.DataFrame()


        page = st.sidebar.radio("Select Page", ["General", "Raydium"])


        if page == "General":
            st.subheader("General Table")
            table = st.table(general_df)
            while True:
                async for result in transport.subscribe(
                    gql("""
                    subscription {
                        Solana {
                            General: DEXTradeByTokens {
                                Block {
                                    Time
                                }
                                Trade {
                                    Amount
                                    Price
                                    Currency {
                                        Symbol
                                        Name
                                    }
                                    Side {
                                        Amount
                                        Currency {
                                            Symbol
                                            Name
                                            MetadataAddress
                                        }
                                    }
                                    Dex {
                                        ProgramAddress
                                        ProtocolFamily
                                        ProtocolName
                                    }
                                    Market {
                                        MarketAddress
                                    }
                                    Order {
                                        LimitAmount
                                        LimitPrice
                                        OrderId
                                    }
                                    PriceInUSD
                                }
                            }
                        }
                    }
                    """)
                ):
                    rewards_data = result.data
                    if rewards_data and 'Solana' in rewards_data:
                        solana_data = rewards_data['Solana']
                        if 'General' in solana_data and solana_data['General']:
                            general_trades = solana_data['General']
                            for trade in general_trades:
                                general_data = pd.json_normalize(trade['Trade'])
                                general_df = pd.concat([general_df, general_data], ignore_index=True)


                    with st.spinner('Updating data...'):
                        table.table(general_df)  # Update the table with the latest data


        elif page == "Raydium":
            st.subheader("Raydium Table")
            table = st.table(raydium_df)
            while True:
                async for result in transport.subscribe(
                    gql("""
                    subscription {
                        Solana {
                            Raydium: DEXTrades(
                                where: {Trade: {Dex: {ProgramAddress: {is: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"}}}}
                            ) {
                                Trade {
                                    Dex {
                                        ProgramAddress
                                        ProtocolName
                                    }
                                    Buy {
                                        Account {
                                            Address
                                        }
                                        Amount
                                        Currency {
                                            Symbol
                                            Name
                                        }
                                        PriceAgaistSellCurrency: Price
                                        PriceInUSD
                                    }
                                    Sell {
                                        Account {
                                            Address
                                        }
                                        Amount
                                        Currency {
                                            Symbol
                                            Name
                                        }
                                        PriceAgaistBuyCurrency: Price
                                        PriceInUSD
                                    }
                                }
                                Block {
                                    Time
                                    Height
                                }
                                Transaction {
                                    Signature
                                }
                            }
                        }
                    }
                    """)
                ):
                    rewards_data = result.data
                    if rewards_data and 'Solana' in rewards_data:
                        solana_data = rewards_data['Solana']
                        if 'Raydium' in solana_data and solana_data['Raydium']:
                            raydium_trades = solana_data['Raydium']
                            for trade in raydium_trades:
                                raydium_data = pd.json_normalize(trade['Trade'])
                                raydium_df = pd.concat([raydium_df, raydium_data], ignore_index=True)


                    with st.spinner('Updating data...'):
                        table.table(raydium_df)  # Update the table with the latest data


    finally:
        await transport.close()


def main():
    st.title("Solana DEX General & Raydium Data Dashboard")
    asyncio.run(run_subscription())


if __name__ == "__main__":
    main()
