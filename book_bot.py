import os
import csv
import inspect
from datetime import date, datetime, timedelta
from enum import Enum
from collections import defaultdict
from textwrap import dedent
import itertools
import logging
import discord
import tempfile
import json
import pprint
from typing import Optional
from discord.utils import get

logging.basicConfig(level=logging.INFO)

from discord.ext import commands
from db import init_tables, Store
import common
from common import TMW_GUILD_ID, make_ordinal

help_command = commands.DefaultHelpCommand(no_category='Commands')
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='bc! ', help_command=help_command, intents=intents)

_ADMIN_ID = 297606972092710913
_ADMIN_ROLE_IDS = None
_DB_NAME = 'main.db'
store = None


VN_BOARD = 927651314882715678
VN2_BOARD = 1026455413975162890
VN3_BOARD = 1108098978878333009
VN4_BOARD = 1200062710231076974
MANGA_BOARD = 927651315595751424
NOVEL_BOARD = 927651316447215686
VIDYA_BOARD = 927651317143453756
JOSEI_BOARD = 927651317579649075
ALL_BOARD = 927651653472092161

def _set_globals():
    environment = os.environ.get('ENV')
    is_prod = environment == 'prod'
    global _ADMIN_ROLE_IDS
    global _DB_NAME
    if is_prod:
        print("Running on prod")
        _ADMIN_ROLE_IDS = [
            793988624181231616, # `In charge of VN Club role
            809103744042139688, # In charge of manga club
            850122172026060842, # Book club role
            906620777623846932, # Video game leader role,
            1014573384216084480, # 3d club leader role
            627149592579801128, # Moderator
            927261770308005949, #Josei club leader role
            110930694670123008, #In charge of josei club
        ]
        _DB_NAME = 'prod.db'
    else:
        print(f"Running on {environment}")
        _ADMIN_ROLE_IDS = [
            813144788714520586, # jmaa server
            813294637958823986, # test server
        ]
        _DB_NAME = 'main.db'


@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')
    _set_globals()
    print(f'Initing tables on {_DB_NAME}')
    global store
    store = Store(_DB_NAME)
    init_tables(_DB_NAME)
    await update_info()
    print('Done initing tables')


@bot.command(name='new_club', help="Create a new club")
async def on_message(ctx, name: str, code: str):
    if not common.has_role(ctx.author, _ADMIN_ROLE_IDS):
        return

    club = store.get_club(ctx.guild.id, code)
    if club:
        await ctx.send(f'Existing club already exists under {club.name}')
        return

    store.new_club(ctx.guild.id, name, code)
    await ctx.send(f'New club "{name}" created with code {code}')


@bot.command(name='new_book', help="Add a new book")
async def on_message(ctx, club_code: str, name: str, code: str, points: float = 2.0, created_at: str = None):
    if not common.has_role(ctx.author, _ADMIN_ROLE_IDS):
        return

    club_code = club_code.upper()
    code = code.upper()

    if not created_at:
        created_at = datetime.combine(date.today(), datetime.min.time())
    else:
        created_at = datetime.strptime(created_at, '%Y-%m-%d')

    club = store.get_club(ctx.guild.id, club_code)
    if not club:
        await ctx.send(f'Unknown club code {club_code}')
        return
    print(club)
    book = store.get_book(ctx.guild.id, code)
    if book:
        await ctx.send(f'Book with code {code} already exists!')
        return
    print(book)
    store.new_book(ctx.guild.id, club_code, name, code, points, created_at)


    await ctx.send(f'New book "{name}" added with code {code} worth {points:g} points')
    await update_club_message(club_code)


@bot.command(name='delete_book', help="Delete a book")
async def on_message(ctx, code: str):
    if not common.has_role(ctx.author, _ADMIN_ROLE_IDS):
        return

    code = code.upper()
    book = store.get_book(ctx.guild.id, code)
    if not book:
        await ctx.send(f'No such book exists!')
        return

    store.delete_book(ctx.guild.id, code)
    await ctx.send(f'Deleted book {code}')
    await update_club_message(book.club_code)


@bot.command(name='finished', help="Mark someone who has finished a book")
async def on_message(ctx, member: discord.Member, book_code: str, points: float = None):
    if not common.has_role(ctx.author, _ADMIN_ROLE_IDS):
        return

    discord_guild_id = ctx.guild.id
    discord_user_id = member.id
    book_code = book_code.upper()

    book = store.get_book(discord_guild_id, book_code)
    if not book:
        await ctx.send(f'Unknown book code {book_code}.')
        return

    activity = store.get_activity(discord_guild_id, discord_user_id, book_code)
    if activity:
        await ctx.send(f'{member} has already finished {book_code}.')
        return

    if not points:
        points = book.points

    store.new_activity(discord_guild_id, discord_user_id, book.club_code, book_code, points)
    await ctx.send(f'{member.mention} has finished {book_code} {common.emoji("Yay")}')
    await update_club_message(book.club_code)
    await update_club_message(None) # Update all


@bot.command(name='books', help='Club overview with books and associated readers')
async def on_message(ctx, club_code: str):
    if ctx.author == bot.user:
        return

    club_code = club_code.upper()
    club = store.get_club(ctx.guild.id, club_code)
    if not club:
        return await ctx.channel.send(f"Unknown club {club_code}")

    activities = store.get_activities_by_club(ctx.guild.id, club_code)
    print(activities)
    readers_by_book = defaultdict(list)
    for activity in activities:
        user_id = activity.discord_user_id
        readers_by_book[(activity.book_name, activity.book_code, activity.created_at)].append(user_id)

    title = f'**{club.name}**'
    embed = discord.Embed(title=title)
    for (book_name, book_code, created_at), readers in readers_by_book.items():
        # readers = [await common.get_member(bot, ctx.guild, r) for r in readers]

        # reader_str = ', '.join(f'{r.display_name}' for r in readers if r)
        if readers:
            reader_str = f'{len(readers)} members'
        else:
            reader_str = 'No members'
        embed.add_field(
            name=f'**{book_name} [{book_code}]({format_created_at(created_at)})**', value=reader_str, inline=False)

    await ctx.channel.send(embed=embed)


@bot.command(name='users', help='Users overview. Shows users and their book list')
async def on_message(ctx, club_code: str):
    if ctx.author == bot.user:
        return

    club = store.get_club(ctx.guild.id, club_code)
    if not club:
        await ctx.channel.send(f"Unknown club {club_code}")

    activities = store.get_activities_by_club(ctx.guild.id, club_code)


    books_by_user = defaultdict(list)
    for activity in activities:
        books_by_user[activity.discord_user_id].append((activity.book_code, activity.points))

    def sum_book_points(book_points):
        return sum(pts for _, pts in book_points)

    books_by_user = dict(sorted(books_by_user.items(),
                                key=lambda item: sum_book_points(item[1]), reverse=True))

    title = f'**{club.name}**'
    def book_points_to_str(book_points):
        book_list = ', '.join(f'{code}({points:g})' for code, points in book_points)
        total_points = sum(points for _, points in book_points)
        return f'{book_list}: **{total_points:g} pts**'


    description = "\n".join(f'<@!{user}>: {book_points_to_str(books)}' for user, books in books_by_user.items())
    embed = discord.Embed(title=title, description=description)
    await ctx.channel.send(embed=embed)


@bot.command(name='user', help='Single user overview.')
async def on_message(ctx, discord_user_id: str):
    if ctx.author == bot.user:
        return

    activities = store.get_activities_by_user(ctx.guild.id, discord_user_id)
    if not activities:
        await ctx.channel.send(f"No activities for {discord_user_id}")
        return

    books_by_club = defaultdict(list)
    for activity in activities:
        books_by_club[activity.club_code].append((activity.book_code, activity.points, activity.book_points))

    def sum_book_points(book_points):
        return sum(pts for _, pts in book_points)

    # title = f'**<@{discord_user_id}>**'
    embed = discord.Embed()
    embed.add_field(name='**User**', value=f'<@!{discord_user_id}>')
    embed.add_field(name='**Books**', value=len(activities))
    embed.add_field(name='**Points**', value=sum(a.points for a in activities))

    for club_code, books in books_by_club.items():
        books_str = '\n'.join(f'{book_code}: {str(points) + " [partial]" if points < book_points else str(points) + " [extra]" if points > book_points else book_points}' for book_code, points, book_points in books)
        embed.add_field(name=f'**{club_code}**', value=books_str)

    await ctx.channel.send(embed=embed)


@bot.command(name='score', help='Show scoreboard')
async def on_message(ctx, club_code: str = None):
    if ctx.author == bot.user:
        return

    club_name = ''
    if club_code:
        club_name = store.get_club(ctx.guild.id, club_code).name
    else:
        club_name = ctx.guild.name

    leaderboard = store.get_scoreboard(ctx.guild.id, club_code)
    title = f'**{club_name} Scoreboard**'
    leaderboard_msg = "\n".join([f'<@!{user_id}>: {points:g} pts' for user_id, points in leaderboard[:20]])
    embed = discord.Embed(title=title, description=leaderboard_msg)
    await ctx.channel.send(embed=embed)


@bot.command(name='book', help="Show book info and who has read it")
async def on_message(ctx, book_code: str):
    if ctx.author == bot.user:
        return
    discord_guild_id = ctx.guild.id

    book_code = book_code.upper()
    book = store.get_book(discord_guild_id, book_code)
    if not book:
        await ctx.send(f'Unknown book code {book_code}.')
        return

    activities = store.get_activities_by_book(discord_guild_id, book_code)
    users = "\n".join(f"<@!{act.discord_user_id}>" for act in activities)
    embed = discord.Embed(title=book.name)
    embed.add_field(name='**Club**', value=book.club_code)
    embed.add_field(name='**Points**', value=book.points)
    embed.add_field(name='**Month**', value=format_created_at(book.created_at))
    embed.add_field(name='**Readers**', value=len(activities))
    embed.add_field(name='**Users**', value=users, inline=False)
    await ctx.channel.send(embed=embed)


def format_created_at(created_at):
    return created_at.strftime('%b %Y')



async def update_info():
    await update_club_message('VN3')
    await update_club_message('VN4')
    await update_club_message('MANGA')
    await update_club_message('NOVEL')
    await update_club_message('VIDYA')
    await update_club_message('JOSEI')
    await update_club_message(None)
async def update_club_message(club_code):
    print(f"Updating {club_code}")
    info = {
        'VN4': VN4_BOARD,
        'VN3': VN3_BOARD,
        'MANGA': MANGA_BOARD,
        'NOVEL': NOVEL_BOARD,
        'VIDYA': VIDYA_BOARD,
        'JOSEI': JOSEI_BOARD,
        None: ALL_BOARD,
    }
    msg_id = info[club_code]

    channel = bot.get_channel(924744340809601094)
    #for _ in range(1):
     #   await channel.send(embed=discord.Embed(title='placeholder'))
      #  return

    msg = await channel.fetch_message(msg_id)
    guild = await bot.fetch_guild(TMW_GUILD_ID)
    leaderboard = store.get_scoreboard(TMW_GUILD_ID, club_code)

    title = f'**{club_code or "All"} Scoreboard**'
    async def leaderboard_row(user_id, points, rank):
        user = await common.get_member(bot, guild, user_id)
        display_name = user.display_name if user else 'Unknown'
        return f'**{make_ordinal(rank)} {display_name}**: {common.millify(points)}pts'

    leaderboard_msg = "\n".join([await leaderboard_row(user_id, pts, i+1) for i, (user_id, pts) in enumerate(leaderboard[:10])])
    embed = discord.Embed(title=title, description=leaderboard_msg)

    content = ''
    if club_code:
        past_books = store.get_books(TMW_GUILD_ID, club_code)
        past_books_str = ', '.join(f'**{b.name}**[{b.code}]' for b in past_books[:50])
        content = f"Past picks: {past_books_str}"
    await msg.edit(content=content, embed=embed)

bot.run('')

