
CRATES_TABLE = {
    "butterfly_crate": {
        "display_name": "Butterfly Crate",
        "icon": "<:butterflycrate:1522265725476147233>",
        "obtainable": False,
        "obtained_through": {
            "work": {
                "obtainable": False,
                "chance": 0.1  # 10% chance
            },
            "crime": {
                "obtainable": False,
                "chance": 0.30  # 30% chance
            },
        },
        "rewards": {
            "120_won": {
                "display_name": "120 Won",
                "type": "won_currency",
                "amount": 120,
                "weight": 50  # 50% chance
            },
            "kitty_launcher": {
                "display_name": "Kitty Launcher Role",
                "type": "role",
                "duplicated_reward_amount": 1000,
                "duplicated_reward_type": "won_currency",
                "role_id": 1522266009954812076,
                "weight": 3  # 3% chance
            },
            "200_won": {
                "display_name": "200 Won",
                "type": "won_currency",
                "amount": 200,
                "weight": 23.5  # 47% chance (so everything adds up to 100)
            },
            "10_stardust": {
                "display_name": "10 Stardust",
                "type": "stardust_currency",
                "amount": 10,
                "weight": 23.5  # 47% chance (so everything adds up to 100)
            },
        }
    },
    "loot_crate": {
        "display_name": "Loot Crate",
        "icon": "<:commoncrateicon:1522578131620331660>",
        "obtainable": True,
        "obtained_through": {
            "work": {
                "obtainable": True,
                "chance": 0.1  # 10% chance
            },
            "crime": {
                "obtainable": True,
                "chance": 0.30  # 30% chance
            },
        },
        "rewards": {
            "120_won": {
                "display_name": "120 Won",
                "type": "won_currency",
                "amount": 120,
                "weight": 50  # 50% chance
            },
            "angelic_crate_1": {
                "display_name": "Angelic Crate",
                "type": "crate",
                "crate": "angelic_crate",
                "amount": 1,
                "weight": 3  # 3% chance
            },
            "angelic_crate_2": {
                "display_name": "Angelic Crate (x3)",
                "type": "crate",
                "crate": "angelic_crate",
                "amount": 3,
                "weight": 1  # 1% chance
            },
            "200_won": {
                "display_name": "200 Won",
                "type": "won_currency",
                "amount": 200,
                "weight": 22.5  # 47% chance (so everything adds up to 100)
            },
            "10_stardust": {
                "display_name": "10 Stardust",
                "type": "stardust_currency",
                "amount": 10,
                "weight": 23.5  # 47% chance (so everything adds up to 100)
            },
        }
    },
    "angelic_crate": {
        "display_name": "Angelic Crate",
        "icon": "<:heavenlyCrate:1522645854916186202>",
        "obtainable": True,
        "obtained_through": {
            "work": {
                "obtainable": True,
                "chance": 0.05  # 5% chance
            },
            "crime": {
                "obtainable": True,
                "chance": 0.07  # 7% chance
            },
        },
        "rewards": {
            "750_won": {
                "display_name": "750 Won",
                "type": "won_currency",
                "amount": 750,
                "weight": 50  # 50% chance
            },
            "1250_won": {
                "display_name": "1250 Won",
                "type": "won_currency",
                "amount": 1250,
                "weight": 23.5  # 47% chance (so everything adds up to 100)
            },
            "seraphim": {
                "display_name": "Seraphim Role",
                "type": "role",
                "duplicated_reward_amount": 1000,
                "duplicated_reward_type": "won_currency",
                "role_id": 1522646595538128916,
                "weight": 3  # 3% chance
            },
            "50_stardust": {
                "display_name": "50 Stardust",
                "type": "stardust_currency",
                "amount": 50,
                "weight": 23.5  # 47% chance (so everything adds up to 100)
            },
        }
    },
    "pokemon_crate": {
        "display_name": "Pokemon Crate",
        "icon": "<:PokemonCrate:1523436005234053342>",
        "obtainable": True,
        "obtained_through": {
            "store": {
                "purchasable": True,
                "currency_type": "won_currency",
                "cost": 1000
            },
        },
        "rewards": {
            "tapu_koko": {
                "display_name": "Tapu Koko Role",
                "type": "role",
                "duplicated_reward_amount": 1000,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523430414335742113,
                "weight": 0.5  # 1% chance
            },
            "tapu_bulu": {
                "display_name": "Tapu Bulu Role",
                "type": "role",
                "duplicated_reward_amount": 1000,
                "duplicated_reward_type": "won_currency",
                "role_id": 1524409208336945316,
                "weight": 0.5,  # 1% chance
                "limited": True,
                "limited_per_user": 3,
            },
            "iron_valliant": {
                "display_name": "Iron Valliant Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523430203655979049,
                "weight": 3  # 3% chance
            },
            "garchomp": {
                "display_name": "Garchomp Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523429508357554176,
                "weight": 5  # 5% chance
            },
            "incineroar": {
                "display_name": "Incineroar Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523429327600095340,
                "weight": 8  # 8% chance
            },
            "quagsire": {
                "display_name": "Quagsire Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523429218707570780,
                "weight": 12  # 12% chance
            },
            "ledian": {
                "display_name": "Ledian Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523429061542674654,
                "weight": 16  # 16% chance
            },
            "smeargle": {
                "display_name": "Smeargle Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523428739591962674,
                "weight": 17  # 17% chance
            },
            "magikarp": {
                "display_name": "Magikarp Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1523428249944719431,
                "weight": 18  # 18% chance
            },
            "sunkern": {
                "display_name": "Sunkern Role",
                "type": "role",
                "duplicated_reward_amount": 500,
                "duplicated_reward_type": "won_currency",
                "role_id": 1522327064286597361,
                "weight": 20  # 20% chance
            }
        }
    }
}
