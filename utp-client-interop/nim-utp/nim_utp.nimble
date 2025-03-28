# Package

version       = "0.1.0"
author        = "KolbyML"
description   = "A new awesome nimble package"
license       = "MIT"
srcDir        = "src"
bin           = @["nim_utp"]


# Dependencies

requires "nim >= 1.6.12",
  "chronos",
  "eth"

