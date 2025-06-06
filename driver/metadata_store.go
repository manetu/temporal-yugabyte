// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package driver

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/manetu/temporal-yugabyte/utils/gocql"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

const (
	namespaceMetadataRecordName = "temporal-namespace-metadata"
)

const (
	templateNamespaceColumns = `id, name, detail, detail_encoding, notification_version, is_global_namespace`

	templateCreateNamespace = `INSERT INTO namespaces ` +
		`(` + templateNamespaceColumns + `) ` +
		`VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS ELSE ERROR`

	templateGetNamespaceNameById = `SELECT name FROM namespaces WHERE id = ?`

	templateListNamespaces = `SELECT ` + templateNamespaceColumns + ` FROM namespaces`

	templateGetNamespace = templateListNamespaces + ` WHERE name = ?`

	templateGetMetadata = `SELECT notification_version FROM namespaces WHERE name = ?`

	templateUpdateNamespace = `UPDATE namespaces ` +
		`SET detail = ? ,` +
		`detail_encoding = ? ,` +
		`is_global_namespace = ? ,` +
		`notification_version = ? ` +
		`WHERE name = ? `

	templateUpdateMetadataQuery = `UPDATE namespaces ` +
		`SET notification_version = ? ` +
		`WHERE name = ? ` +
		`IF notification_version = ? ELSE ERROR`

	templateDeleteNamespace          = `DELETE FROM namespaces WHERE name = ?`
	templateDeleteNamespaceWithGuard = templateDeleteNamespace +
		` IF EXISTS ELSE ERROR`
)

type (
	MetadataStore struct {
		session            gocql.Session
		logger             log.Logger
		currentClusterName string
	}
)

// NewMetadataStore is used to create an instance of the Namespace MetadataStore implementation
func NewMetadataStore(
	currentClusterName string,
	session gocql.Session,
	logger log.Logger,
) (p.MetadataStore, error) {
	return &MetadataStore{
		currentClusterName: currentClusterName,
		session:            session,
		logger:             logger,
	}, nil
}

func (m *MetadataStore) CreateNamespace(
	ctx context.Context,
	request *p.InternalCreateNamespaceRequest,
) (*p.CreateNamespaceResponse, error) {
	metadata, err := m.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}

	txn := m.session.NewTxn().WithContext(ctx)

	txn.Query(templateCreateNamespace,
		request.ID,
		request.Name,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		metadata.NotificationVersion,
		request.IsGlobal)
	m.updateMetadataTxn(txn, metadata.NotificationVersion)

	err = txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			msg := fmt.Sprintf("Namespace %v already exists", request.ID)
			return nil, serviceerror.NewNamespaceAlreadyExists(msg)
		}

		return nil, err
	}

	return &p.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *MetadataStore) UpdateNamespace(
	ctx context.Context,
	request *p.InternalUpdateNamespaceRequest,
) error {
	txn := m.session.NewTxn().WithContext(ctx)
	txn.Query(templateUpdateNamespace,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		request.IsGlobal,
		request.NotificationVersion,
		request.Name,
	)
	m.updateMetadataTxn(txn, request.NotificationVersion)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return serviceerror.NewUnavailable("UpdateNamespace operation failed because of conditional failure.")
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("UpdateNamespace operation failed. Error: %v", err))
	}

	return nil
}

func (m *MetadataStore) RenameNamespace(
	ctx context.Context,
	request *p.InternalRenameNamespaceRequest,
) error {
	txn := m.session.NewTxn().WithContext(ctx)
	txn.Query(templateDeleteNamespaceWithGuard, request.PreviousName)
	txn.Query(templateCreateNamespace,
		request.Id,
		request.Name,
		request.Namespace.Data,
		request.Namespace.EncodingType.String(),
		request.NotificationVersion,
		request.IsGlobal)
	m.updateMetadataTxn(txn, request.NotificationVersion)

	err := txn.Exec()
	if err != nil {
		if gocql.ConflictError(err) {
			return serviceerror.NewUnavailable("RenameNamespace operation failed because of conditional failure.")
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("RenameNamespace operation failed. Error: %v", err))
	}

	return nil
}

func (m *MetadataStore) GetNamespace(
	ctx context.Context,
	request *p.GetNamespaceRequest,
) (*p.InternalGetNamespaceResponse, error) {
	var query gocql.Query
	var err error
	var detail []byte
	var detailEncoding string
	var notificationVersion int64
	var isGlobalNamespace bool

	if len(request.ID) > 0 && len(request.Name) > 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name specified in request.Namespace.")
	} else if len(request.ID) == 0 && len(request.Name) == 0 {
		return nil, serviceerror.NewInvalidArgument("GetNamespace operation failed.  Both ID and Name are empty.")
	}

	handleError := func(name string, ID string, err error) error {
		identity := name
		if gocql.IsNotFoundError(err) {
			if len(ID) > 0 {
				identity = ID
			}
			return serviceerror.NewNamespaceNotFound(identity)
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("GetNamespace operation failed. Error %v", err))
	}

	namespace := request.Name
	if len(request.ID) > 0 {
		query = m.session.Query(templateGetNamespaceNameById, request.ID).WithContext(ctx)
		err = query.Scan(&namespace)
		if err != nil {
			return nil, handleError(request.Name, request.ID, err)
		}
	}

	query = m.session.Query(templateGetNamespace, namespace).WithContext(ctx)
	err = query.Scan(
		nil,
		nil,
		&detail,
		&detailEncoding,
		&notificationVersion,
		&isGlobalNamespace,
	)

	if err != nil {
		return nil, handleError(request.Name, request.ID, err)
	}

	return &p.InternalGetNamespaceResponse{
		Namespace:           p.NewDataBlob(detail, detailEncoding),
		IsGlobal:            isGlobalNamespace,
		NotificationVersion: notificationVersion,
	}, nil
}

func (m *MetadataStore) ListNamespaces(
	ctx context.Context,
	request *p.InternalListNamespacesRequest,
) (*p.InternalListNamespacesResponse, error) {
	query := m.session.Query(templateListNamespaces).WithContext(ctx)
	pageSize := request.PageSize
	nextPageToken := request.NextPageToken
	response := &p.InternalListNamespacesResponse{}

	for {
		iter := query.PageSize(pageSize).PageState(nextPageToken).Iter()
		skippedRows := 0

		for {
			var name string
			var detail []byte
			var detailEncoding string
			var notificationVersion int64
			var isGlobal bool
			if !iter.Scan(
				nil,
				&name,
				&detail,
				&detailEncoding,
				&notificationVersion,
				&isGlobal,
			) {
				// done iterating over all namespaces in this page
				break
			}

			// do not include the metadata record
			if name == namespaceMetadataRecordName {
				skippedRows++
				continue
			}
			response.Namespaces = append(response.Namespaces, &p.InternalGetNamespaceResponse{
				Namespace:           p.NewDataBlob(detail, detailEncoding),
				IsGlobal:            isGlobal,
				NotificationVersion: notificationVersion,
			})
		}
		if len(iter.PageState()) > 0 {
			nextPageToken = iter.PageState()
		} else {
			nextPageToken = nil
		}
		if err := iter.Close(); err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNamespaces operation failed. Error: %v", err))
		}

		if len(nextPageToken) == 0 {
			// No more records in DB.
			break
		}
		if skippedRows == 0 {
			break
		}
		pageSize = skippedRows
	}

	response.NextPageToken = nextPageToken
	return response, nil
}

func (m *MetadataStore) DeleteNamespace(
	ctx context.Context,
	request *p.DeleteNamespaceRequest,
) error {
	var name string
	query := m.session.Query(templateGetNamespaceNameById, request.ID).WithContext(ctx)
	err := query.Scan(&name)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil
		}
		return err
	}

	query = m.session.Query(templateDeleteNamespace, name).WithContext(ctx)
	return query.Exec()
}

func (m *MetadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *p.DeleteNamespaceByNameRequest,
) error {
	query := m.session.Query(templateDeleteNamespace, request.Name).WithContext(ctx)
	err := query.Exec()
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil
		}
		return err
	}
	return err
}

func (m *MetadataStore) GetMetadata(
	ctx context.Context,
) (*p.GetMetadataResponse, error) {
	var notificationVersion int64
	query := m.session.Query(templateGetMetadata, namespaceMetadataRecordName).WithContext(ctx)
	err := query.Scan(&notificationVersion)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			// this error can be thrown in the very beginning,
			// i.e. when namespaces is initialized
			return &p.GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, err
	}
	return &p.GetMetadataResponse{NotificationVersion: notificationVersion}, nil
}

func (m *MetadataStore) updateMetadataTxn(
	txn *gocql.Txn,
	notificationVersion int64,
) {
	var nextVersion int64 = 1
	var currentVersion *int64
	if notificationVersion > 0 {
		nextVersion = notificationVersion + 1
		currentVersion = &notificationVersion
	}
	txn.Query(templateUpdateMetadataQuery,
		nextVersion,
		namespaceMetadataRecordName,
		currentVersion,
	)
}

func (m *MetadataStore) GetName() string {
	return yugabytePersistenceName
}

func (m *MetadataStore) Close() {
	if m.session != nil {
		m.session.Close()
	}
}
