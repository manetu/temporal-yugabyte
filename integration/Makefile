# Copyright © Manetu, Inc.  All rights reserved

SUBDIRS := core clojure

.PHONY: all clean
all clean: $(SUBDIRS)

all: TARGET=all
clean: TARGET=clean

$(SUBDIRS): force
	@ $(MAKE) -s -C $@ $(TARGET)

.PHONY: force
	force :;
