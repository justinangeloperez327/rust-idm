#define AppName "Rust IDM"
[Setup]
AppId={{174B6C75-992E-4E7B-9503-A98FD5F7A121}
AppName={#AppName}
AppVersion=0.1.0
AppPublisher=Rust IDM
DefaultDirName={localappdata}\Programs\RustIDM
DefaultGroupName=Rust IDM
OutputDir=..\..\dist
OutputBaseFilename=RustIDM-v0.1.0-win64-setup
Compression=lzma2
SolidCompression=yes
PrivilegesRequired=lowest
WizardStyle=modern
ArchitecturesAllowed=x64compatible
[Files]
Source: "..\..\target\release\rust-idm-desktop.exe"; DestDir: "{app}"; DestName: "RustIDM.exe"; Flags: ignoreversion
Source: "..\..\docs\RELEASE-v0.1.0.md"; DestDir: "{app}"; DestName: "README.txt"; Flags: ignoreversion
[Icons]
Name: "{group}\Rust IDM"; Filename: "{app}\RustIDM.exe"
Name: "{autodesktop}\Rust IDM"; Filename: "{app}\RustIDM.exe"; Tasks: desktopicon
[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; Flags: unchecked
