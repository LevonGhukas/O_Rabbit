package common

import "testing"

func TestResolveUnderProjectRejectsTraversal(t *testing.T) {
	if _, err := ResolveUnderProject("/root/O_Rabbit", "../.env.master"); err == nil {
		t.Fatal("expected traversal rejection")
	}
}

func TestResolveUnderProjectRejectsRelativeProjectDir(t *testing.T) {
	if _, err := ResolveUnderProject("relative/project", ".env.master"); err == nil {
		t.Fatal("expected relative project_dir rejection")
	}
}
