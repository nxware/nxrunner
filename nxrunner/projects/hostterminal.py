"""
VS-Code-Extension "Host Terminal" fuer den CodeServer der Projects.

Die Extension stellt das Terminalprofil "Host" bereit (VS-Code-Pseudoterminal), das die Shell des
nxrunner-Terminal-Runners auf dem Host ueber die Ops output, input und resize bedient. Die Verbindung laeuft vom
Extension-Host im Container zu nxrunner (Einstellung nxrunner.hostApiUrl, vom CodeServer in die .code-workspace-Datei
geschrieben), nicht vom Browser aus. build_vsix erzeugt daraus ein installierbares .vsix.
"""
import json
import os
import zipfile
from importlib import resources

EXT_PUBLISHER = 'nxrunner'
EXT_NAME = 'hostterminal'
EXT_ID = f'{EXT_PUBLISHER}.{EXT_NAME}'

# Ressource in nxrunner/resources -> Pfad im .vsix
FILES = {
    'hostterminal.package.json': 'extension/package.json',
    'hostterminal.extension.js': 'extension/extension.js',
}

CONTENT_TYPES = (
    '<?xml version="1.0" encoding="utf-8"?>\n'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension=".json" ContentType="application/json"/>'
    '<Default Extension=".js" ContentType="application/javascript"/>'
    '<Default Extension=".vsixmanifest" ContentType="text/xml"/>'
    '</Types>\n')


def resource(name: str) -> str:
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


def vsix_manifest(package: dict) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<PackageManifest Version="2.0.0" xmlns="http://schemas.microsoft.com/developer/vsx-schema/2011">\n'
        '  <Metadata>\n'
        f'    <Identity Language="en-US" Id="{package["name"]}" Version="{package["version"]}" '
        f'Publisher="{package["publisher"]}"/>\n'
        f'    <DisplayName>{package["displayName"]}</DisplayName>\n'
        f'    <Description xml:space="preserve">{package["description"]}</Description>\n'
        '    <Properties>\n'
        f'      <Property Id="Microsoft.VisualStudio.Code.Engine" Value="{package["engines"]["vscode"]}"/>\n'
        '    </Properties>\n'
        '  </Metadata>\n'
        '  <Installation><InstallationTarget Id="Microsoft.VisualStudio.Code"/></Installation>\n'
        '  <Dependencies/>\n'
        '  <Assets>\n'
        '    <Asset Type="Microsoft.VisualStudio.Code.Manifest" Path="extension/package.json" Addressable="true"/>\n'
        '  </Assets>\n'
        '</PackageManifest>\n')


def build_vsix(path: str) -> str:
    """ schreibt die Extension als .vsix nach path und gibt path zurueck """
    package = json.loads(resource('hostterminal.package.json'))
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', CONTENT_TYPES)
        z.writestr('extension.vsixmanifest', vsix_manifest(package))
        for name, target in FILES.items():
            z.writestr(target, resource(name))
    return path
