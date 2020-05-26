from __future__ import unicode_literals, division, print_function, absolute_import

import argparse
import io
import sys

import pkg_resources
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker

from sqlacodegen.codegen import CodeGenerator, BackRefDescription

import pathlib
import csv

def load_backref_csv(file_path):
    """
    Gets a back relationship dict from a csv file.
    """
    if not file_path.exists():
        raise Exception("File {} does not exist".format(file_path))
    backref_relationships = {}
    with open(file_path, 'r') as fh:
        csv_reader = csv.reader(fh)
        for line in csv_reader:
            if not line:
                continue
            sc = line[0]
            tc = line[1]
            name = line[2]
            if len(line) >= 4:
                pj = line[3]
            else:
                pj = None

            backref_relationships[(sc, tc)] = BackRefDescription(sc, tc, name, pj)
    return backref_relationships


def main():
    parser = argparse.ArgumentParser(
        description='Generates SQLAlchemy model code from an existing database.')
    parser.add_argument('url', nargs='?', help='SQLAlchemy url to the database')
    parser.add_argument('--version', action='store_true', help="print the version number and exit")
    parser.add_argument('--schema', help='load tables from an alternate schema')
    parser.add_argument('--tables', help='tables to process (comma-separated, default: all)')
    parser.add_argument('--noviews', action='store_true', help="ignore views")
    parser.add_argument('--noindexes', action='store_true', help='ignore indexes')
    parser.add_argument('--noconstraints', action='store_true', help='ignore constraints')
    parser.add_argument('--nojoined', action='store_true',
                        help="don't autodetect joined table inheritance")
    parser.add_argument('--noinflect', action='store_true',
                        help="don't try to convert tables names to singular form")
    parser.add_argument('--noclasses', action='store_true',
                        help="don't generate classes, only tables")
    parser.add_argument('--nocomments', action='store_true', help="don't render column comments")
    parser.add_argument('--outfile', help='file to write output to (default: stdout)')
    parser.add_argument('--table_backref_file', help='CSV file with source table, target table, and back relationship name')
    parser.add_argument(
        "--add_version",
        action='store_true',
        help='Adds version to printed model as a constant. Version is found querying the "version" table. There is no current way of modifying this besides chanfing the script.',
    )
    args = parser.parse_args()

    if args.version:
        version = pkg_resources.get_distribution('sqlacodegen').parsed_version
        print(version.public)
        return
    if not args.url:
        print('You must supply a url\n', file=sys.stderr)
        parser.print_help()
        return

    # Use reflection to fill in the metadata
    engine = create_engine(args.url)
    metadata = MetaData(engine)
    session = sessionmaker(bind=engine)()
    tables = args.tables.split(',') if args.tables else None
    metadata.reflect(engine, args.schema, not args.noviews, tables)

    # take care of the backref hack
    backrefs_tables = None
    if args.table_backref_file:
        backref_file = pathlib.Path(args.table_backref_file)
        backrefs_tables = load_backref_csv(backref_file)

    # Check if we need to add the version
    version = None
    if args.add_version:
        version = session.query(metadata.tables["version"]).all()[0][0]

    # Write the generated model code to the specified file or standard output
    outfile = io.open(args.outfile, 'w', encoding='utf-8') if args.outfile else sys.stdout
    generator = CodeGenerator(metadata, args.noindexes, args.noconstraints, args.nojoined,
                              args.noinflect, args.noclasses, backrefs_tables, model_version=version, nocomments=args.nocomments)
    generator.render(outfile)
