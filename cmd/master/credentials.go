package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/user"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/auth"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

const (
	credentialsUsage  = "usage: orabbit-master credentials create|list|revoke [flags]"
	cliActorType      = "cli"
	cliCommandTimeout = time.Minute
)

// runCredentials implements `orabbit-master credentials create|list|revoke`.
// It edits the master database directly, which is how the first admin
// credential is bootstrapped. Unlike rotate-master-key it deliberately does
// not take the master singleton lock: SQLite WAL allows it to run beside a
// serving master, and it never touches fenced state held in memory (see
// fencedTables). A new credential takes effect on the master's next request.
func runCredentials(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, credentialsUsage)
		return exitConfig
	}
	switch args[0] {
	case "create":
		return credentialsCreate(args[1:], stdout, stderr)
	case "list":
		return credentialsList(args[1:], stdout, stderr)
	case "revoke":
		return credentialsRevoke(args[1:], stdout, stderr)
	}
	fmt.Fprintf(stderr, "unknown credentials command %q\n%s\n", args[0], credentialsUsage)
	return exitConfig
}

func openCredentialStore(ctx context.Context, dbPath string, stderr io.Writer) (*db.Store, int) {
	st, err := db.Open(ctx, db.Config{Path: dbPath}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		fmt.Fprintln(stderr, "open db:", err)
		return nil, exitFailure
	}
	return st, exitOK
}

func credentialsCreate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("credentials create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", loadMasterConfigFromEnv().DBPath, "SQLite DB path")
	principal := fs.String("principal", "", "Name recorded in the audit log")
	kind := fs.String("kind", string(auth.KindAutomation), "human or automation")
	scopes := fs.String("scopes", "", "Comma-separated scopes: "+auth.ScopeList())
	ttl := fs.Duration("ttl", 0, "Lifetime, e.g. 720h (0 = never expires)")
	description := fs.String("description", "", "Free-text note")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	actor := cliActor()
	cred, tok, err := auth.Issue(auth.IssueRequest{
		Principal: *principal, Kind: *kind, Scopes: splitList(*scopes), TTL: *ttl, Description: *description,
	}, actor, time.Now())
	if err != nil {
		fmt.Fprintln(stderr, "credentials create:", err)
		return exitConfig
	}
	ctx, cancel := context.WithTimeout(context.Background(), cliCommandTimeout)
	defer cancel()
	st, code := openCredentialStore(ctx, *dbPath, stderr)
	if st == nil {
		return code
	}
	defer st.Close()
	audit := cliAudit("credential.create", cred.ID, actor, map[string]any{
		"principal": cred.Principal, "kind": cred.Kind, "scopes": cred.Scopes, "expires_at": cred.ExpiresAt,
	})
	if err := st.CreateAPICredential(ctx, cred, &audit); err != nil {
		fmt.Fprintln(stderr, "credentials create:", err)
		return exitFailure
	}
	fmt.Fprintf(stdout, "credential %s created for %s (scopes: %s)\n", cred.ID, cred.Principal, strings.Join(cred.Scopes, ","))
	fmt.Fprintf(stdout, "token (shown once, store it now): %s\n", tok.Value)
	return exitOK
}

func credentialsList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("credentials list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", loadMasterConfigFromEnv().DBPath, "SQLite DB path")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	ctx, cancel := context.WithTimeout(context.Background(), cliCommandTimeout)
	defer cancel()
	st, code := openCredentialStore(ctx, *dbPath, stderr)
	if st == nil {
		return code
	}
	defer st.Close()
	creds, err := st.ListAPICredentials(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "credentials list:", err)
		return exitFailure
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tPRINCIPAL\tKIND\tSTATUS\tEXPIRES\tSCOPES")
	now := time.Now()
	for _, c := range creds {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", c.ID, c.Principal, c.Kind, credentialStatus(c, now), c.ExpiresAt, strings.Join(c.Scopes, ","))
	}
	_ = tw.Flush()
	return exitOK
}

func credentialsRevoke(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("credentials revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", loadMasterConfigFromEnv().DBPath, "SQLite DB path")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: orabbit-master credentials revoke [--db path] <credential-id>")
		return exitConfig
	}
	id := fs.Arg(0)
	ctx, cancel := context.WithTimeout(context.Background(), cliCommandTimeout)
	defer cancel()
	st, code := openCredentialStore(ctx, *dbPath, stderr)
	if st == nil {
		return code
	}
	defer st.Close()
	audit := cliAudit("credential.revoke", id, cliActor(), nil)
	if err := st.RevokeAPICredential(ctx, id, &audit); err != nil {
		fmt.Fprintln(stderr, "credentials revoke:", err)
		if errors.Is(err, db.ErrCredentialNotFound) {
			return exitConfig
		}
		return exitFailure
	}
	fmt.Fprintf(stdout, "credential %s revoked\n", id)
	return exitOK
}

func credentialStatus(c db.APICredential, now time.Time) string {
	switch {
	case c.RevokedAt != "":
		return "revoked"
	case !c.Active(now):
		return "expired"
	}
	return "active"
}

func cliActor() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return cliActorType + ":" + u.Username
	}
	if name := os.Getenv("USER"); name != "" {
		return cliActorType + ":" + name
	}
	return cliActorType
}

func cliAudit(action, credentialID, actor string, metadata map[string]any) db.AuditRecord {
	rec := db.AuditRecord{
		ActorType: cliActorType, ActorID: actor, Principal: actor,
		Action: action, ResourceType: "credential", ResourceID: credentialID,
	}
	if metadata != nil {
		rec.MetadataJSON = mustJSON(metadata)
	}
	return rec
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}
