import click

import json

from Client.s2mclient import S2MClient

def list_obj(metadata):
    
    a = []
    for o in metadata["objects"]:
        a.append(o["id"])
    return a

def list_buckets(l):

    a = []
    for b in l:
        a.append(b["id"])
    return a

def list_tree(ctx,l):

    a = []
    for d in l:
        b = d["id"]
        c = S2MClient(ctx.obj['REPO'])
        metadata,code = c.get_metadata(b)
        if code != 200:
            return None
        a.append({"bucket":b,"obj":list_obj(metadata)})
    return a

@click.group()
@click.option('--repo','-r', help='Repository\'s url')
@click.pass_context
def main(ctx,repo):
    """
    Simple CLI for interact with a S2M Server
    """
    if repo is None:
        try:
            conf = open("./.s2m-cli.config",'r')
            conf = json.loads(conf.read())
            repo = conf["REPO"]
        except:
            click.echo("Error: Missing repository's url")
            return
        

    ctx.ensure_object(dict)
    ctx.obj['REPO'] = repo

@main.command()
@click.pass_context
def save(ctx):
    """
    Save repository's url preference
    """
    conf = open("./.s2m-cli.config",'w')
    conf.write(json.dumps({"REPO":ctx.obj['REPO']}))

@main.command()
@click.pass_context
def ls(ctx):
    """List all buckets in a repository"""
    try:
        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)

        l,code = c.list()

        if code != 200 or l is None:
            click.echo("Connection Error: "+str(code))
            return

        if l == []:
            click.echo("Repository Empty")
            return
        else:
            l = list_buckets(l)
            click.echo("Buckets in this repository:")
            for e in l:
                click.echo("- "+e)
    except:
        click.echo("Network Error")

@main.command()
@click.pass_context
def tree(ctx):
    """List all buckets and their objects in a repository"""
    try:
        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)

        l,code = c.list()

        if code != 200 or l is None:
            click.echo("Connection Error: "+str(code))

        if l == []:
            click.echo("Repository Empty")
        else:
            l = list_tree(ctx,l)
            if l is None:
                click.echo("Network Error")
                return

            click.echo("Repository's tree:")
            for e in l:
                click.echo("- "+e["bucket"])
                for o in e["obj"]:
                    click.echo("  * "+o)
    except:
        click.echo("Network Error")

@main.command()
@click.argument('bucket')
@click.option('--obj',"-o", help="Object's id.")
@click.option('--metadata',"-m", is_flag=True, help="Get only resource's metadata.")
@click.pass_context
def get(ctx,bucket,obj,metadata):
    """Get information about a bucket or a object"""
    try:
        
        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)
    
        if metadata or obj is None:
            bucket_metadata,code = c.get_metadata(bucket,obj)
            if code != 200:
                click.echo("Connection Error: "+str(code))
                return

            if metadata:
                click.echo(bucket_metadata)
            else:
                l = list_obj(bucket_metadata)
                if l == []:
                    click.echo("Bucket Empty")
                else:
                    click.echo("Objects in "+bucket+" bucket:")
                    for e in l:
                        click.echo("- "+e)
        else:
            data,code = c.get_obj(bucket,obj)
            if code != 200:
                click.echo("Connection Error: "+str(code))
                return
            if data["data"] == '':
                data["data"] = "Object Empty"

            click.echo(data["data"])
    except:
        click.echo("Network Error")

@main.command()
@click.argument('bucket')
@click.option('--obj',"-o", help="Object's id.")
@click.option('--file',"-f", help="File path.", type=click.File("r"))
@click.option('--data',"-d", help="Data.")
@click.pass_context
def add(ctx,bucket,obj,file,data):
    """Add a bucket or a object"""
    try:
        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)

        if obj is None:
            metadata,code = c.add_bucket(bucket)
            if code == 201:
                click.echo("Bucket created!")
                click.echo(metadata)
            else:
                click.echo("Connection Error: "+str(code))
        else:
            if file is not None:
                data = file.read()
            elif data is None:
                data = ""
        
            metadata,code = c.add_obj(bucket,obj,data)

            if code == 201:
                click.echo("Object created!")
                click.echo(metadata)
            else:
                click.echo("Connection Error: "+str(code))
    except:
        click.echo("Network Error")

@main.command()
@click.argument('bucket')
@click.option('--obj',"-o", help="Object's id.")
@click.pass_context
def delete(ctx,bucket,obj):
    """Delete a bucket or a object"""
    try:
        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)

        b, code = c.delete(bucket)

        if b:
            if obj is None:
                click.echo("Bucket "+bucket+" deleted succesfully")
            else:
                click.echo("Object "+obj+" in bucket "+bucket+" deleted succesfully")
        else:
            click.echo("Connection Error: "+str(code))

    except:
        click.echo("Network Error")

@main.command()
@click.argument('bucket')
@click.option('--obj',"-o", help="Object's id.")
@click.option('--file',"-f", help="File path.", type=click.File("r"))
@click.option('--data',"-d", help="Data.")
@click.pass_context
def update(ctx,bucket,obj,file,data):
    """Update a bucket or a object"""
    try:

        if(obj is None):
            click.echo("Update a bucket is not allowed")
            return

        repo = ctx.obj['REPO']
        click.echo("Connecting to repository @ "+repo+"...")
        c = S2MClient(repo)

        if file is not None:
            data = file.read()
        elif data is None:
            data = ""
        
        metadata,code = c.update_obj(bucket,obj,data)

        if code == 200:
            click.echo("Object updated!")
            click.echo(metadata)
        else:
            click.echo("Connection Error: "+str(code))
    except:
        click.echo("Network Error")

if __name__ == "__main__":
    main(obj={})