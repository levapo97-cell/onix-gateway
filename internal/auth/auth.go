// Package auth: login de un solo usuario con JWT (acceso) para el panel.
// Contraseña verificada con bcrypt si hay hash; si no, comparación directa (solo dev).
package auth

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type Auth struct {
	user         string
	password     string // dev: texto plano si no hay hash
	passwordHash string // bcrypt (producción)
	secret       []byte
	ttl          time.Duration
}

func New(user, password, passwordHash, secret string) *Auth {
	if user == "" {
		user = "jefe"
	}
	if password == "" && passwordHash == "" {
		password = "onix" // solo dev
	}
	if secret == "" {
		secret = "dev-insecure-secret-change-me"
	}
	return &Auth{user: user, password: password, passwordHash: passwordHash, secret: []byte(secret), ttl: 24 * time.Hour}
}

// Login valida credenciales y, si son correctas, devuelve un JWT firmado.
func (a *Auth) Login(user, pass string) (string, error) {
	if user != a.user {
		return "", errors.New("credenciales inválidas")
	}
	ok := false
	if a.passwordHash != "" {
		ok = bcrypt.CompareHashAndPassword([]byte(a.passwordHash), []byte(pass)) == nil
	} else {
		ok = pass == a.password
	}
	if !ok {
		return "", errors.New("credenciales inválidas")
	}
	claims := jwt.RegisteredClaims{
		Subject:   a.user,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(a.ttl)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(a.secret)
}

// valid comprueba un token.
func (a *Auth) valid(token string) bool {
	if token == "" {
		return false
	}
	t, err := jwt.Parse(token, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("alg inesperado")
		}
		return a.secret, nil
	})
	return err == nil && t.Valid
}

// Middleware protege rutas: exige Authorization: Bearer <jwt>.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !a.valid(tok) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"no autorizado"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ValidToken expone la validación para el WS (token por query param).
func (a *Auth) ValidToken(token string) bool { return a.valid(token) }
